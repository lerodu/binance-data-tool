"""Shared helpers: S3 bucket discovery, paths, kline-CSV parsing, time grid."""

import io
import os
import re
import time
import urllib.parse
import urllib.request
import zipfile

import numpy as np

# the website root serves HTML; the raw S3 XML listing lives at the bucket endpoint
LIST_HOST = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
BASE = "https://data.binance.vision/data/spot/monthly/klines"
PREFIX = "data/spot/monthly/klines/"

COLS = ["open", "high", "low", "close", "volume", "quote_vol", "trades",
        "taker_buy_base", "taker_buy_quote"]
SRC_IDX = [1, 2, 3, 4, 5, 7, 8, 9, 10]  # kline-csv column indices for COLS

SYMS_FILE = "symbols_{year}.json"  # written by download, read by consolidate


def months(year):
    return [f"{year}-{m:02d}" for m in range(1, 13)]


def base_ms(year):
    return int(np.datetime64(f"{year}-01-01T00:00:00", "ms").astype("int64"))


_UNIT_MS = {"s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}


def interval_ms(interval):
    """Fixed-duration interval string ('1m','5m','1h','1d','1w',...) -> milliseconds.
    Used as the bar length when computing a candle's row index."""
    unit = interval[-1]
    if unit == "M":
        raise ValueError("monthly interval '1M' has no fixed length; not supported")
    if unit not in _UNIT_MS:
        raise ValueError(f"unsupported interval {interval!r}")
    return int(interval[:-1]) * _UNIT_MS[unit]


def periods_in_year(year, interval):
    """Number of fixed-length bars in a year = the array height (leap-year correct
    via the ms span). Also bounds the valid row indices."""
    span = base_ms(year + 1) - base_ms(year)
    p = interval_ms(interval)
    return (span + p - 1) // p  # ceil so a final partial bar still gets a slot


def to_ms(ts):
    """Normalize candle open_time to milliseconds PER ENTRY so it shares the unit
    of base_ms before differencing.

    Binance open_time units are not uniform (seconds/ms/us/ns) and can even vary
    within one file, so the unit is inferred per row from its magnitude:
      >=1e18 ns, >=1e15 us, >=1e12 ms, >=1e9 s.
    """
    ts = ts.astype("int64")
    return np.where(ts >= 10**18, ts // 1_000_000,
           np.where(ts >= 10**15, ts // 1_000,
           np.where(ts >= 10**12, ts,
                    ts * 1_000)))


def zip_path(cache, sym, interval, ym):
    return os.path.join(cache, sym, f"{sym}-{interval}-{ym}.zip")


def _list_page(marker):
    """One page of the S3 bucket listing (XML). Returns (prefixes, next_marker)."""
    params = {"delimiter": "/", "prefix": PREFIX}
    if marker:
        params["marker"] = marker
    url = f"{LIST_HOST}/?" + urllib.parse.urlencode(params)
    for attempt in range(5):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                xml = r.read().decode()
            break
        except Exception:
            time.sleep(2 * (attempt + 1))
    else:
        raise RuntimeError(f"S3 list failed: {url}")
    prefixes = re.findall(r"<CommonPrefixes><Prefix>([^<]+)</Prefix></CommonPrefixes>", xml)
    truncated = "<IsTruncated>true</IsTruncated>" in xml
    nm = re.search(r"<NextMarker>([^<]+)</NextMarker>", xml)
    if nm:
        next_marker = nm.group(1)
    elif truncated and prefixes:
        next_marker = prefixes[-1]  # AWS omits NextMarker w/ delimiter -> use last key
    else:
        next_marker = None
    return prefixes, next_marker


def discover_symbols(symbols_override=None):
    """Scan the entire S3 bucket (paginated) so DELISTED symbols are included."""
    if symbols_override:
        return [s.strip().upper() for s in symbols_override.split(",")]
    syms, marker, pages = [], None, 0
    while True:
        prefixes, marker = _list_page(marker)
        for p in prefixes:                       # e.g. data/spot/monthly/klines/BTCUSDT/
            syms.append(p.rstrip("/").split("/")[-1])
        pages += 1
        print(f"  listed page {pages}: {len(syms)} symbols so far", flush=True)
        if not marker:
            break
    return sorted(set(syms))


def parse_zip(path, year, interval):
    """Return (bar_idx int array, values float array (k,9)) from one zip.
    bar_idx is the row position in the year's grid: (open_time - year_start) / bar."""
    with zipfile.ZipFile(path) as z:
        raw = z.read(z.namelist()[0])
    arr = np.genfromtxt(io.BytesIO(raw), delimiter=",", dtype=np.float64,
                        usecols=[0] + SRC_IDX, invalid_raise=False)
    if arr.ndim == 1:
        arr = arr[None, :]
    arr = arr[~np.isnan(arr[:, 0])]  # drop header/garbage rows
    ms = to_ms(arr[:, 0])            # per-entry s/ms/us/ns -> ms
    idx = ((ms - base_ms(year)) // interval_ms(interval)).astype("int64")
    vals = arr[:, 1:].astype(np.float32)
    return idx, vals
