"""Build a dense (symbols, bars, cols) .npy + meta.json for a bounded slice — the
research export produced by `consolidate ... --sink npy`. The 1s->interval resample
(a derived, research-time choice) lives here, never in the faithful SQL store.
Columns are only those actually downloaded (no taker-buy).

Two front-ends share one aggregation core:
  - from_cache(): read the downloaded candle Parquet directly (no SQL needed)
  - from_sql():   read an already-ingested SQLite candle table
"""

import glob
import json
import os
import sqlite3

import numpy as np

from ..common import interval_ms, to_ms

COLS_OUT = ["open", "high", "low", "close", "volume", "quote_vol", "trades"]
SRC_COLS = ["open", "high", "low", "close", "volume", "volume_quote", "trade_count"]


def _grid(start, end, interval):
    base = int(np.datetime64(f"{start}T00:00:00", "ms").astype("int64"))
    end_ms = int(np.datetime64(f"{end}T00:00:00", "ms").astype("int64")) + 86_400_000
    step = interval_ms(interval)
    return base, step, (end_ms - base + step - 1) // step


def _aggregate(groups, base, step, T):
    """groups: {(dex,coin): (k,8) array of [ts,o,h,l,c,v,vq,tc]} -> (S,T,7) array."""
    symbols = [f"{c}@{d}" for (d, c) in sorted(groups)]
    arr = np.full((len(symbols), T, len(COLS_OUT)), np.nan, dtype=np.float32)
    for s, key in enumerate(sorted(groups)):
        g = groups[key]
        order = np.argsort(g[:, 0], kind="stable"); g = g[order]   # ensure ts-sorted
        bar = ((to_ms(g[:, 0]) - base) // step).astype(np.int64)
        ok = (bar >= 0) & (bar < T); bar, g = bar[ok], g[ok]
        if not len(bar):
            continue
        o, h, l, c, v, vq, tc = (g[:, i] for i in range(1, 8))
        col = arr[s]
        np.fmax.at(col[:, 1], bar, h)
        np.fmin.at(col[:, 2], bar, l)
        ubar = np.unique(bar)
        for ci, srcv in ((4, v), (5, vq), (6, tc)):
            col[ubar, ci] = 0.0
            np.add.at(col[:, ci], bar, srcv)
        first = np.unique(bar, return_index=True)[1]
        last = len(bar) - 1 - np.unique(bar[::-1], return_index=True)[1]
        col[bar[first], 0] = o[first]
        col[ubar, 3] = c[last]
    return symbols, arr


def _save(symbols, arr, base, step, interval, start, end, out, extra):
    np.save(out, arr)
    meta = {"symbols": symbols, "columns": COLS_OUT, "base_ms": base, "interval": interval,
            "interval_ms": step, "bars": int(arr.shape[1]), "start": start, "end": end,
            "shape": list(arr.shape), "resampled_from": "1s", **extra}
    json.dump(meta, open(out.replace(".npy", ".meta.json"), "w"))
    print(f"materialized {out}  shape={arr.shape}  ({len(symbols)} symbols)", flush=True)
    return out


def _candle_files(cache, source_name, dexes):
    pats = [os.path.join(cache, source_name, "by_dex", "*", "candles", "1s", "date=*", "candles.parquet"),
            os.path.join(cache, source_name, "global", "candles", "1s", "date=*", "candles.parquet")]
    files = sorted(f for p in pats for f in glob.glob(p))
    if dexes:
        files = [f for f in files if any(f"/by_dex/{d}/" in f or (d == "global" and "/global/" in f)
                                         for d in dexes)]
    return files


def from_cache(source_name, cache, dexes=None, coins=None, start=None, end=None,
               interval="1m", out="hl_klines.npy"):
    import pyarrow.parquet as pq
    base, step, T = _grid(start, end, interval)
    coinset = set(coins) if coins else None
    groups = {}
    cols = ["coin", "dex", "timestamp"] + SRC_COLS
    for path in _candle_files(cache, source_name, dexes):
        tbl = pq.read_table(path, columns=cols)
        coin = np.array(tbl.column("coin").to_pylist())
        dex = np.array(tbl.column("dex").to_pylist())
        ts = np.asarray(tbl.column("timestamp").to_numpy(zero_copy_only=False), dtype=np.float64)
        num = np.column_stack([np.asarray(tbl.column(c).to_numpy(zero_copy_only=False), dtype=np.float64)
                               for c in SRC_COLS])
        for (d, c) in {(d, c) for d, c in zip(dex, coin)}:
            if coinset and c not in coinset:
                continue
            m = (dex == d) & (coin == c)
            block = np.column_stack([ts[m], num[m]])
            groups.setdefault((d, c), []).append(block)
    groups = {k: np.vstack(v) for k, v in groups.items()}
    if not groups:
        raise SystemExit("no candle data in cache for the requested slice")
    symbols, arr = _aggregate(groups, base, step, T)
    return _save(symbols, arr, base, step, interval, start, end, out,
                 {"source": source_name, "from": "cache"})


def from_sql(db, table="reservoir_candles", dexes=None, coins=None, start=None, end=None,
             interval="1m", out="hl_klines.npy"):
    base, step, T = _grid(start, end, interval)
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    tsc = next((r[1] for r in con.execute(f'PRAGMA table_info("{table}")')
                if r[1] in ("timestamp", "ts_ms", "ts", "open_time")), None)
    if tsc is None:
        raise SystemExit(f"no timestamp column in {table}")
    where, params = [], []
    if dexes:
        where.append(f"dex IN ({','.join('?' * len(dexes))})"); params += dexes
    if coins:
        where.append(f"coin IN ({','.join('?' * len(coins))})"); params += coins
    wsql = (" WHERE " + " AND ".join(where)) if where else ""
    sel = f'SELECT dex, coin, "{tsc}", {",".join(SRC_COLS)} FROM "{table}"{wsql}'
    groups = {}
    for row in con.execute(sel, params):
        groups.setdefault((row[0], row[1]), []).append(row[2:])
    con.close()
    if not groups:
        raise SystemExit("no candle rows matched the slice")
    groups = {k: np.array(v, dtype=np.float64) for k, v in groups.items()}
    symbols, arr = _aggregate(groups, base, step, T)
    return _save(symbols, arr, base, step, interval, start, end, out,
                 {"source": "sql", "table": table})
