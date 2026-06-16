"""Source-agnostic downloader: fetch raw archive files into a local cache.

Mirrors the Binance pipeline's guarantees, re-keyed from (symbol, month) to
(source, datatype, dex, asset_class, date[, coin]): resumable by final-name
existence, `.missing` markers for absent keys, `.part` temp + atomic rename, and
an integrity check (parquet footer / lz4 decode) before a file reaches its final
name. The cache mirrors the S3 key layout under <cache>/<source>/<key>.
"""

import io
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm

from .base import dates_in_range


def cache_path(cache, source_name, key):
    return os.path.join(cache, source_name, key)


def _valid(data, fmt):
    """True iff bytes are a complete file of the expected format."""
    try:
        if fmt == "parquet":
            import pyarrow.parquet as pq
            return pq.ParquetFile(io.BytesIO(data)).metadata.num_rows >= 0
        if fmt == "lz4":
            import lz4.frame
            lz4.frame.decompress(data)
            return True
    except Exception:
        return False
    return False


def _download_one(source, dt, key, cache):
    """Returns 'skip'|'ok'|'missing'|'err' for one S3 key."""
    path = cache_path(cache, source.name, key)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return "skip"
    if os.path.exists(path + ".missing"):
        return "missing"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    for attempt in range(3):
        try:
            data = source.get(key)
        except Exception:
            continue                                  # auth/network -> retry, leave unmarked
        if data is None:
            open(path + ".missing", "w").close()
            return "missing"
        if not _valid(data, dt.fmt):
            continue
        tmp = path + ".part"
        with open(tmp, "wb") as f:
            f.write(data); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)
        return "ok"
    return "err"


def _tasks(source, dt, dexes, asset_class, dates, coins):
    """Enumerate (key,) tasks. For per-coin datatypes, coins must be provided or
    discovered per (dex, day)."""
    keys = []
    dexes = dexes or ([""] if not source.by_dex else ["hyperliquid"])
    for dex in dexes:
        for day in dates:
            if dt.per_coin:
                day_coins = coins or source.coins_for(dt, dex, asset_class, day)
                for coin in day_coins:
                    keys.append(dt.key(dex, asset_class, day, coin))
            else:
                keys.append(dt.key(dex, asset_class, day, coin=""))
    return keys


def run(source, datatype, dexes=None, asset_class="perp", start=None, end=None,
        coins=None, cache="hl_cache", workers=8, recheck_missing=False):
    dt = source.datatypes.get(datatype)
    if dt is None:
        raise SystemExit(f"{source.name} has no datatype {datatype!r}; "
                         f"available: {', '.join(source.datatypes)}")
    dates = dates_in_range(start, end)
    os.makedirs(os.path.join(cache, source.name), exist_ok=True)
    if recheck_missing:
        cleared = 0
        for root, _, files in os.walk(os.path.join(cache, source.name)):
            for fn in files:
                if fn.endswith(".missing"):
                    os.remove(os.path.join(root, fn)); cleared += 1
        print(f"recheck-missing: cleared {cleared} markers", flush=True)

    keys = _tasks(source, dt, dexes, asset_class, dates, coins)
    print(f"download {source.name}/{datatype}: {len(keys)} keys, {workers} workers", flush=True)
    counts = {"ok": 0, "skip": 0, "missing": 0, "err": 0}
    manifest_keys = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_download_one, source, dt, k, cache): k for k in keys}
        bar = tqdm(as_completed(futs), total=len(keys), unit="key", smoothing=0.02)
        for fut in bar:
            r = fut.result(); counts[r] += 1
            if r in ("ok", "skip"):
                manifest_keys.append(futs[fut])
            bar.set_postfix(counts)
    period = f"{dates[0]}_{dates[-1]}" if dates else "all"
    man = os.path.join(cache, source.name, f"{datatype}_manifest_{period}.json")
    json.dump({"source": source.name, "datatype": datatype, "asset_class": asset_class,
               "dexes": dexes, "start": start, "end": end, "keys": sorted(manifest_keys)},
              open(man, "w"))
    print(f"download done: {counts}  (manifest -> {man})", flush=True)
    if counts["err"]:
        print(f"  {counts['err']} errors (auth/network/corrupt) — re-run to retry", flush=True)
    return counts
