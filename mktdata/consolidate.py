"""Consolidate cached kline zips into one aligned 3D .npy per year.

Year-level resumable (skips a year whose .npy + complete meta already exist) and
optionally parallel: with --workers>1, symbols are parsed in a process pool and
each worker writes its own disjoint row into the shared memmap.
"""

import json
import os
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
from numpy.lib.format import open_memmap
from tqdm import tqdm

from .common import (COLS, SYMS_FILE, base_ms, interval_ms, months,
                     parse_zip, periods_in_year, zip_path)


def _universe(cache, year):
    f = os.path.join(cache, SYMS_FILE.format(year=year))
    if os.path.exists(f):
        return json.load(open(f))["symbols"]
    subs = [d for d in sorted(os.listdir(cache))  # fallback: scan cache dirs
            if os.path.isdir(os.path.join(cache, d))]
    print(f"  (no {SYMS_FILE.format(year=year)}; scanned {len(subs)} cache dirs)")
    return subs


def _fill_col(sym, T, cache, interval, yms, year):
    """Parse a symbol's months into a (T, 9) NaN-filled column array."""
    col = np.full((T, len(COLS)), np.nan, dtype=np.float32)
    parsed, failures = 0, []
    for ym in yms:
        path = zip_path(cache, sym, interval, ym)
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            continue
        try:
            idx, vals = parse_zip(path, year, interval)
            parsed += 1
        except Exception as e:
            failures.append((sym, ym, str(e)[:40]))
            continue
        ok = (idx >= 0) & (idx < T)
        col[idx[ok]] = vals[ok]
    return col, parsed, failures


# --- worker side: open the shared memmap once per process, write disjoint rows ---
_MM = None


def _init_worker(out):
    global _MM
    _MM = open_memmap(out, mode="r+")


def _work(args):
    s, sym, T, cache, interval, yms, year = args
    col, parsed, failures = _fill_col(sym, T, cache, interval, yms, year)
    _MM[s] = col
    return parsed, failures


def _is_complete(out):
    meta = out.replace(".npy", ".meta.json")
    if not (os.path.exists(out) and os.path.exists(meta)):
        return False
    try:
        return json.load(open(meta)).get("complete") is True
    except Exception:
        return False


def consolidate_year(year, cache, interval="1m", out_prefix="klines", workers=1, force=False):
    out = f"{out_prefix}_{interval}_{year}.npy"  # encodes interval so 1m/1h don't collide
    if not force and _is_complete(out):
        print(f"{year}: already complete -> {out} (use --force to rebuild)", flush=True)
        return
    T = periods_in_year(year, interval)
    yms = months(year)
    universe = _universe(cache, year)
    symbols = [s for s in universe
               if any(os.path.exists(zip_path(cache, s, interval, ym))
                      and os.path.getsize(zip_path(cache, s, interval, ym)) > 0
                      for ym in yms)]
    S = len(symbols)
    if S == 0:
        print(f"{year}: no cached data, skipping", flush=True)
        return
    print(f"{year}: ({S}, {T}, {len(COLS)}) float32 -> {out} "
          f"(~{S*T*len(COLS)*4/1e9:.1f} GB), {S}/{len(universe)} symbols, workers={workers}",
          flush=True)
    mm = open_memmap(out, mode="w+", dtype=np.float32, shape=(S, T, len(COLS)))
    del mm  # workers (or the sequential loop below) own the writes

    files_parsed, failures = 0, []
    if workers > 1:
        tasks = [(s, sym, T, cache, interval, yms, year) for s, sym in enumerate(symbols)]
        with ProcessPoolExecutor(max_workers=workers, initializer=_init_worker,
                                 initargs=(out,)) as ex:
            futs = [ex.submit(_work, t) for t in tasks]
            for fut in tqdm(as_completed(futs), total=len(futs), desc=str(year), unit="sym"):
                p, f = fut.result()
                files_parsed += p; failures += f
    else:
        mm = open_memmap(out, mode="r+")
        for s, sym in enumerate(tqdm(symbols, desc=str(year), unit="sym")):
            col, p, f = _fill_col(sym, T, cache, interval, yms, year)
            mm[s] = col
            files_parsed += p; failures += f
        mm.flush()

    meta = {"symbols": symbols, "columns": COLS, "base_ms": base_ms(year),
            "interval": interval, "interval_ms": interval_ms(interval), "bars": T,
            "year": year, "shape": [S, T, len(COLS)],
            "files_parsed": files_parsed, "parse_failures": failures, "complete": True}
    mp = out.replace(".npy", ".meta.json")
    json.dump(meta, open(mp + ".tmp", "w"))
    os.replace(mp + ".tmp", mp)  # atomic completion marker, written last
    print(f"saved {out} + meta.json  ({files_parsed} files parsed, {len(failures)} failed)",
          flush=True)
    if failures:
        print(f"  WARNING: {len(failures)} zips failed to parse -> data gaps. Delete those "
              f"cached files and re-run the download to refetch.", flush=True)


def run(years, cache, interval="1m", out_prefix="klines", workers=1, force=False):
    for year in years:
        consolidate_year(year, cache, interval, out_prefix, workers, force)
