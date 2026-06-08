"""Parallel, resumable, checksum-verified download of monthly kline zips."""

import hashlib
import io
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm

from .common import BASE, SYMS_FILE, discover_symbols, months, zip_path


def expected_sha(url):
    """Fetch the .CHECKSUM sidecar and return the expected SHA256 hex, or None."""
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url + ".CHECKSUM", timeout=60) as r:
                return r.read().decode().split()[0].lower()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None  # genuinely no checksum for this file
            time.sleep(1 + attempt)
        except Exception:
            time.sleep(1 + attempt)
    return None


def valid_zip(data):
    """True iff `data` is a complete, CRC-intact zip with at least one member.
    Catches truncation / corruption / HTML-error-body even without a checksum."""
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            return bool(z.namelist()) and z.testzip() is None
    except Exception:
        return False


def download_one(sym, ym, cache, interval):
    """Fetch+verify one monthly zip if absent. Returns 'ok'|'skip'|'missing'|'err'.

    A zip is only moved to its final name after passing verification, so any file
    present at its final name is, by construction, correct -> resume just checks
    existence and never re-hashes."""
    path = zip_path(cache, sym, interval, ym)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return "skip"  # present at final name => already verified when written
    os.makedirs(os.path.dirname(path), exist_ok=True)
    q = urllib.parse.quote(sym, safe="")  # handle non-ASCII / odd symbols
    url = f"{BASE}/{q}/{interval}/{q}-{interval}-{ym}.zip"
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=120) as r:
                data = r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                open(path + ".missing", "w").close()  # mark so we don't retry
                return "missing"
            time.sleep(1 + attempt); continue
        except Exception:
            time.sleep(1 + attempt); continue
        # mandatory verification BEFORE the file reaches its final name:
        #   CRC (testzip) always; SHA256 vs .CHECKSUM whenever one is published
        if not valid_zip(data):
            time.sleep(1 + attempt); continue
        want = expected_sha(url)
        if want and hashlib.sha256(data).hexdigest().lower() != want:
            time.sleep(1 + attempt); continue  # authenticity mismatch -> redownload
        tmp = f"{path}.part"  # write to temp, fsync, then atomic rename
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        return "ok"
    return "err"  # not marked .missing, so a re-run retries it


def run(years, cache, interval="1m", workers=12, symbols=None, recheck_missing=False):
    syms = discover_symbols(symbols)
    print(f"{len(syms)} spot symbols, years {years[0]}-{years[-1]}", flush=True)
    os.makedirs(cache, exist_ok=True)
    if recheck_missing:  # clear 404 markers so those months are retried
        cleared = 0
        for root, _, files in os.walk(cache):
            for fn in files:
                if fn.endswith(".missing"):
                    os.remove(os.path.join(root, fn)); cleared += 1
        print(f"recheck-missing: cleared {cleared} markers", flush=True)
    # symbol list is year-independent; consolidate reads it per year
    for year in years:
        json.dump({"symbols": syms, "year": year, "interval": interval},
                  open(os.path.join(cache, SYMS_FILE.format(year=year)), "w"))

    tasks = [(s, ym) for year in years for ym in months(year) for s in syms
             if not os.path.exists(zip_path(cache, s, interval, ym) + ".missing")]
    print(f"download: {len(tasks)} (symbol,month) zips, {workers} workers", flush=True)
    counts = {"ok": 0, "skip": 0, "missing": 0, "err": 0}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(download_one, s, ym, cache, interval): (s, ym)
                for s, ym in tasks}
        bar = tqdm(as_completed(futs), total=len(tasks), unit="zip", smoothing=0.02)
        for fut in bar:
            counts[fut.result()] += 1
            bar.set_postfix(counts)  # live ok/skip/missing/err with rate + ETA
    print(f"download done: {counts}", flush=True)
    if counts["err"]:
        print(f"  {counts['err']} errors (corrupt/network) — re-run to retry them", flush=True)
    return counts
