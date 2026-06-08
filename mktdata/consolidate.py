"""Consolidate cached kline zips into one aligned 3D .npy per year."""

import json
import os

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


def consolidate_year(year, cache, interval="1m", out_prefix="klines"):
    T = periods_in_year(year, interval)  # array height = number of bars in the year
    yms = months(year)
    universe = _universe(cache, year)
    # keep only symbols with >=1 present zip this year -> right-sized file
    symbols = [s for s in universe
               if any(os.path.exists(zip_path(cache, s, interval, ym))
                      and os.path.getsize(zip_path(cache, s, interval, ym)) > 0
                      for ym in yms)]
    S = len(symbols)
    out = f"{out_prefix}_{year}.npy"
    if S == 0:
        print(f"{year}: no cached data, skipping", flush=True)
        return
    print(f"{year}: ({S}, {T}, {len(COLS)}) float32 -> {out} "
          f"(~{S*T*len(COLS)*4/1e9:.1f} GB), {S}/{len(universe)} symbols have data",
          flush=True)
    mm = open_memmap(out, mode="w+", dtype=np.float32, shape=(S, T, len(COLS)))
    files_parsed = parse_fail = 0
    failures = []
    for s, sym in enumerate(tqdm(symbols, desc=str(year), unit="sym")):
        col = np.full((T, len(COLS)), np.nan, dtype=np.float32)
        for ym in yms:
            path = zip_path(cache, sym, interval, ym)
            if not os.path.exists(path) or os.path.getsize(path) == 0:
                continue
            try:
                idx, vals = parse_zip(path, year, interval)
                files_parsed += 1
            except Exception as e:
                parse_fail += 1
                failures.append((sym, ym))
                tqdm.write(f"  parse fail {sym} {ym}: {e}")
                continue
            ok = (idx >= 0) & (idx < T)
            col[idx[ok]] = vals[ok]
        mm[s] = col
    mm.flush()
    # meta.json is the completion marker: a .npy without it is incomplete/partial
    meta = {"symbols": symbols, "columns": COLS, "base_ms": base_ms(year),
            "interval": interval, "interval_ms": interval_ms(interval), "bars": T,
            "year": year, "shape": [S, T, len(COLS)],
            "files_parsed": files_parsed, "parse_failures": failures, "complete": True}
    meta_path = out.replace(".npy", ".meta.json")
    json.dump(meta, open(meta_path + ".tmp", "w"))
    os.replace(meta_path + ".tmp", meta_path)  # atomic
    print(f"saved {out} + meta.json  ({files_parsed} files parsed, {parse_fail} failed)",
          flush=True)
    if parse_fail:
        print(f"  WARNING: {parse_fail} zips failed to parse -> data gaps. Delete those "
              f"cached files and re-run the download to refetch.", flush=True)


def run(years, cache, interval="1m", out_prefix="klines"):
    for year in years:
        consolidate_year(year, cache, interval, out_prefix)
