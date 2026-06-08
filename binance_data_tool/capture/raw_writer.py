"""Append-only gzipped JSONL writer with crash-safe flushing and file rotation.

The raw event log is the ground truth: every websocket message is written
verbatim with a local receive timestamp. Files rotate on a time interval so a
long run produces a sequence of complete files (a crash damages at most the
open one) instead of a single giant file. Records are self-contained JSONL,
so a replay just reads the files in timestamp order.

Record line: {"t": <recv unix>, "v": "<exchange>", "m": <message>[, "k": ..., "sym": ...]}
"""

import gzip
import json
import os
import threading
import time
from datetime import datetime, timezone


class RawWriter:
    def __init__(self, out_dir, prefix, rotate_min=60, flush_every=500):
        os.makedirs(out_dir, exist_ok=True)
        self.out_dir = out_dir
        self.prefix = prefix
        self.rotate_s = rotate_min * 60  # 0 disables rotation
        self.flush_every = flush_every
        self.lock = threading.Lock()
        self.count = 0
        self.f = None
        self.path = None
        self._open()

    def _open(self):
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.path = os.path.join(self.out_dir, f"{self.prefix}_{stamp}.jsonl.gz")
        self.f = gzip.open(self.path, "at")
        self.opened_at = time.monotonic()

    def write(self, venue, msg, kind=None, sym=None):
        rec = {"t": time.time(), "v": venue, "m": msg}
        if kind:
            rec["k"] = kind
        if sym:
            rec["sym"] = sym
        line = json.dumps(rec, separators=(",", ":"))
        with self.lock:
            if self.rotate_s and time.monotonic() - self.opened_at >= self.rotate_s:
                self.f.close()
                self._open()
            self.f.write(line + "\n")
            self.count += 1
            if self.count % self.flush_every == 0:
                self.f.flush()  # Z_SYNC_FLUSH: readable up to here on crash

    def flush(self):
        with self.lock:
            if self.f:
                self.f.flush()

    def close(self):
        with self.lock:
            if self.f:
                self.f.close()
                self.f = None

    def current_size_mb(self):
        try:
            return os.path.getsize(self.path) / 1e6
        except OSError:
            return 0.0
