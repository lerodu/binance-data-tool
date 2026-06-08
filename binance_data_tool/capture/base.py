"""Exchange-agnostic capture interface and run loop.

An adapter owns its websocket connection(s), parses messages, logs them
verbatim via the writer, and maintains a minimal in-memory book only for health
display and resync. The shared run() loop handles startup sync, the periodic
health line, the duration / Ctrl-C stop, and graceful shutdown.
"""

import time
from abc import ABC, abstractmethod


class ExchangeCapture(ABC):
    def __init__(self, symbols, writer):
        self.symbols = symbols
        self.writer = writer
        self.dead = False  # set by the adapter on connection loss; stops the run

    @abstractmethod
    def start(self):
        """Launch the websocket / sync threads (non-blocking)."""

    @abstractmethod
    def stop(self):
        """Signal threads to stop."""

    @abstractmethod
    def synced(self) -> bool:
        """True once every requested book is built."""

    @abstractmethod
    def health(self) -> list:
        """Short status strings for the periodic health line."""


def run(adapter, writer, duration=None, sync_timeout=60):
    """Drive an adapter: sync, stream, report, and shut down cleanly."""
    adapter.start()
    print("waiting for books to sync ...", flush=True)
    t0 = time.monotonic()
    while not adapter.synced():
        if time.monotonic() - t0 > sync_timeout:
            print(f"warning: not all books synced after {sync_timeout}s, continuing",
                  flush=True)
            break
        time.sleep(0.2)

    span = f"{duration}s" if duration else "until interrupted (Ctrl-C)"
    print(f"capturing -> {writer.out_dir} ({span})", flush=True)
    start = time.monotonic()
    last_health = start
    try:
        while not adapter.dead:  # a connection loss ends the run; no reconnect
            if duration is not None and time.monotonic() - start >= duration:
                break
            time.sleep(1)  # 1s granularity: prompt stop on outage, accurate duration
            now = time.monotonic()
            if now - last_health >= 10:
                writer.flush()
                print(f"  {int(now - start):6d}s  " + "  ".join(adapter.health())
                      + f"  [{writer.current_size_mb():.1f} MB]", flush=True)
                last_health = now
        if adapter.dead:
            print("connection lost; stopping (no reconnect). Restart to resume.", flush=True)
    except KeyboardInterrupt:
        print("\ninterrupted, shutting down ...", flush=True)
    finally:
        adapter.stop()
        writer.close()
        print(f"done: {writer.count} events -> {writer.out_dir}", flush=True)
