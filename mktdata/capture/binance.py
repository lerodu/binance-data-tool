"""Binance spot order-book capture: depth@100ms diffs + aggTrade, with REST
snapshot sync and update-id gap detection. Ported from the research repo's
capture_raw.py, generalized to many symbols on one connection.

Reconstruction follows Binance's official local-book procedure: buffer diffs,
fetch a REST snapshot, drop diffs older than it, then apply in order. An
update-id gap drops the book to unsynced and triggers a fresh snapshot; the
snapshot is logged (kind="rest_snapshot") so replay can rebuild the book.
"""

import json
import threading
import time
import urllib.request

from websocket import WebSocketApp

from .base import ExchangeCapture

WS_URL = "wss://stream.binance.com:9443/stream"
DEPTH_URL = "https://api.binance.com/api/v3/depth"
SUB_CHUNK = 100  # stream names per SUBSCRIBE message


class _Sym:
    __slots__ = ("bids", "asks", "buffer", "last_id", "synced", "last_msg_t", "n")

    def __init__(self):
        self.bids = {}
        self.asks = {}
        self.buffer = []
        self.last_id = None
        self.synced = False
        self.last_msg_t = 0.0
        self.n = 0


class BinanceSpotOrderbook(ExchangeCapture):
    VENUE = "binance"

    def __init__(self, symbols, writer):
        super().__init__(symbols, writer)
        self.state = {s: _Sym() for s in symbols}
        self.lock = threading.Lock()
        self.stop_flag = False

    # --- ExchangeCapture interface ---
    def start(self):
        threading.Thread(target=self._run_ws, daemon=True).start()
        threading.Thread(target=self._sync_loop, daemon=True).start()

    def stop(self):
        self.stop_flag = True

    def synced(self):
        with self.lock:
            return all(st.synced for st in self.state.values())

    def health(self):
        with self.lock:
            if len(self.state) > 4:  # summarize for wide captures
                n_sync = sum(st.synced for st in self.state.values())
                total = sum(st.n for st in self.state.values())
                return [f"{len(self.state)} symbols, {n_sync} synced, {total} msgs"]
            out = []
            for s, st in self.state.items():
                bb = max(map(float, st.bids), default=float("nan"))
                ba = min(map(float, st.asks), default=float("nan"))
                out.append(f"{s} {(bb + ba) / 2:.2f} (age {time.time() - st.last_msg_t:.1f}s, {st.n})")
            return out

    # --- book maintenance ---
    def _apply(self, st, data):
        for p, q in data["b"]:
            st.bids.pop(p, None) if float(q) == 0.0 else st.bids.__setitem__(p, q)
        for p, q in data["a"]:
            st.asks.pop(p, None) if float(q) == 0.0 else st.asks.__setitem__(p, q)
        st.last_id = data["u"]

    def _on_open(self, ws):
        streams = [f"{s.lower()}@{ch}" for s in self.symbols
                   for ch in ("depth@100ms", "aggTrade")]
        with self.lock:
            for st in self.state.values():
                st.synced = False
                st.buffer = []

        def subscribe():  # chunked so the inbound message stays small
            for i in range(0, len(streams), SUB_CHUNK):
                try:
                    ws.send(json.dumps(
                        {"method": "SUBSCRIBE", "params": streams[i:i + SUB_CHUNK], "id": i + 1}))
                except Exception:
                    return
                time.sleep(0.2)

        threading.Thread(target=subscribe, daemon=True).start()

    def _on_message(self, ws, raw):
        msg = json.loads(raw)
        data = msg.get("data")
        if data is None:
            return  # SUBSCRIBE acks etc.
        sym = data.get("s")
        self.writer.write(self.VENUE, msg, sym=sym)  # log verbatim, first
        st = self.state.get(sym)
        if st is None:
            return
        with self.lock:
            st.n += 1
            if data.get("e") != "depthUpdate":
                return  # aggTrade: logged, not part of the book
            if not st.synced:
                st.buffer.append(data)
                return
            if data["u"] <= st.last_id:
                return
            if data["U"] > st.last_id + 1:
                st.synced = False  # missed events; rebuild from snapshot
                st.buffer = [data]
                print(f"binance {sym}: update gap, resyncing ...", flush=True)
                return
            self._apply(st, data)
            st.last_msg_t = time.time()

    def _sync_loop(self):
        while not self.stop_flag:
            time.sleep(0.2)
            for sym, st in self.state.items():
                if self.stop_flag:
                    return
                with self.lock:
                    first_u = st.buffer[0]["U"] if (not st.synced and st.buffer) else None
                if first_u is None:
                    continue
                try:
                    snap = self._rest_depth(sym)
                except Exception as e:
                    print(f"binance {sym}: snapshot failed ({e}), retrying ...", flush=True)
                    time.sleep(1)
                    continue
                if snap["lastUpdateId"] < first_u:
                    continue  # snapshot predates buffered diffs; fetch a newer one
                self.writer.write(self.VENUE, snap, kind="rest_snapshot", sym=sym)
                with self.lock:
                    st.bids = dict(snap["bids"])
                    st.asks = dict(snap["asks"])
                    st.last_id = snap["lastUpdateId"]
                    for ev in st.buffer:
                        if ev["u"] > st.last_id:
                            self._apply(st, ev)
                    st.buffer = []
                    st.synced = True
                    st.last_msg_t = time.time()
                print(f"binance {sym}: book synced at update id {st.last_id}", flush=True)

    @staticmethod
    def _rest_depth(sym):
        url = f"{DEPTH_URL}?symbol={sym}&limit=5000"
        with urllib.request.urlopen(url, timeout=30) as r:
            return json.load(r)

    def _run_ws(self):
        # single connection: on an unexpected drop we stop the whole capture
        # rather than reconnect, so a log never contains an unmarked gap
        ws = WebSocketApp(WS_URL, on_open=self._on_open, on_message=self._on_message)
        ws.run_forever(ping_interval=15, ping_timeout=10)
        if not self.stop_flag:
            print("binance: connection lost, stopping capture (no reconnect)", flush=True)
            self.dead = True
            self.stop_flag = True
