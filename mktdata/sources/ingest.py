"""Ingest cached archive files into a Sink, faithfully.

Each (source, datatype) lands in its own table with the file's native columns
(no renaming, no cross-source harmonization, nothing derived). The only columns
added are provenance — dex / coin / date parsed from the S3 key — and only when
the file itself doesn't already carry them (per-coin orderbook files don't).
Resumable per key via the sink's done-set.
"""

import glob
import io
import json
import os
import re

import pyarrow as pa

from .download import cache_path

_DATE_RE = re.compile(r"date=(\d{4}-\d{2}-\d{2})")
_DEX_RE = re.compile(r"by_dex/([^/]+)/")


def _provenance(key):
    """dex / coin / date inferred from the S3 key (any may be None)."""
    date = (_DATE_RE.search(key) or [None, None])[1] if _DATE_RE.search(key) else None
    dex = (_DEX_RE.search(key).group(1) if _DEX_RE.search(key) else None)
    base = key.rsplit("/", 1)[-1]
    # per-coin files are named <COIN>.parquet / <COIN>.lz4; collection files are not
    coin = None
    if base not in ("candles.parquet", "fills.parquet", "snapshots.parquet") \
            and not base.endswith(".csv.lz4"):
        coin = base.rsplit(".", 1)[0]
    return {"dex": dex, "coin": coin, "date": date}


def _read(path, fmt):
    if fmt == "parquet":
        import pyarrow.parquet as pq
        return pq.read_table(path)
    if fmt == "lz4":
        import lz4.frame
        import pyarrow.csv as pacsv
        raw = lz4.frame.decompress(open(path, "rb").read())
        # asset_ctxs is CSV; line-delimited-JSON variants would need a json reader here
        return pacsv.read_table(io.BytesIO(raw))
    raise ValueError(fmt)


def _add_provenance(tbl, prov):
    n = tbl.num_rows
    for name in ("dex", "coin", "date"):
        if prov.get(name) is not None and name not in tbl.column_names:
            tbl = tbl.append_column(name, pa.array([prov[name]] * n, pa.string()))
    return tbl


def _keys_from_manifests(cache, source_name, datatype):
    keys = set()
    pat = os.path.join(cache, source_name, f"{datatype}_manifest_*.json")
    for mf in glob.glob(pat):
        keys.update(json.load(open(mf)).get("keys", []))
    return sorted(keys)


def run(source, datatype, sink, cache="hl_cache", keys=None, force=False):
    dt = source.datatypes.get(datatype)
    if dt is None:
        raise SystemExit(f"{source.name} has no datatype {datatype!r}")
    keys = keys or _keys_from_manifests(cache, source.name, datatype)
    if not keys:
        raise SystemExit(f"no downloaded keys for {source.name}/{datatype} in {cache} "
                         f"(run `download` first)")
    print(f"ingest {source.name}/{datatype}: {len(keys)} files -> {dt.table}", flush=True)
    done = skipped = total_rows = 0
    for key in keys:
        if not force and sink.is_done(source.name, datatype, key):
            skipped += 1; continue
        path = cache_path(cache, source.name, key)
        if not (os.path.exists(path) and os.path.getsize(path) > 0):
            continue
        tbl = _add_provenance(_read(path, dt.fmt), _provenance(key))
        sink.ensure_table(dt.table, tbl.schema)
        sink.insert(dt.table, tbl)
        sink.mark_done(source.name, datatype, key, tbl.num_rows)
        done += 1; total_rows += tbl.num_rows
    sink.finalize({"source": source.name, "datatype": datatype, "table": dt.table,
                   "files": done, "rows": total_rows})
    print(f"ingest done: {done} files ({total_rows} rows), {skipped} already-done", flush=True)
    return done, total_rows
