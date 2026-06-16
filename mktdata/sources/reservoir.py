"""Hydromancer Reservoir source descriptor.

s3://hydromancer-reservoir, ap-northeast-1, requester-pays, Parquet, partitioned
by date (orderbook also by coin). dex in {hyperliquid, <HIP-3 deployers>, global}.
Path layout per the Hydromancer docs; verify against the live bucket on first run
(see verify_paths()).
"""

from .base import DataType, Source

BUCKET = "hydromancer-reservoir"
REGION = "ap-northeast-1"
FIRST_DATE = "2025-08-01"          # Hyperliquid perps coverage start (confirm on first pull)


def _candles_key(dex, asset_class, day, coin):
    if dex == "global":
        return f"global/candles/1s/date={day}/candles.parquet"
    return f"by_dex/{dex}/candles/1s/date={day}/candles.parquet"


def _fills_key(dex, asset_class, day, coin):
    ac = asset_class or "perp"
    if dex == "global":
        # global has raw + spot/all; map spot->spot/all else raw
        scope = "spot/all" if ac == "spot" else "raw"
        return f"global/fills/{scope}/date={day}/fills.parquet"
    return f"by_dex/{dex}/fills/{ac}/all/date={day}/fills.parquet"


def _orderbook_key(dex, asset_class, day, coin):
    ac = asset_class or "perps"
    return f"by_dex/{dex}/orderbook/1m/{ac}/date={day}/{coin}.parquet"


def _snapshots_key(dex, asset_class, day, coin):
    return f"snapshots/daily/date={day}/snapshots.parquet"


SOURCE = Source(
    name="hydromancer-reservoir",
    bucket=BUCKET,
    region=REGION,
    requester_pays=True,
    signed=True,
    by_dex=True,
    note="Free Hyperliquid archive (requester-pays). NO funding/OI/mark — use hyperliquid-archive asset-ctxs.",
    datatypes={
        "candles":   DataType("candles",   "parquet", False, "reservoir_candles",   _candles_key, native_interval="1s",
                              note="1s OHLCV+quote_vol+trade_count (no taker-buy). Resampled on consolidate --sink npy."),
        "fills":     DataType("fills",     "parquet", False, "reservoir_fills",     _fills_key,
                              note="Every trade incl. liquidations/ADL/TWAP/builder fills (~27 cols)."),
        "orderbook": DataType("orderbook", "parquet", True,  "reservoir_orderbook", _orderbook_key,
                              note="20-level L2 snapshots at 1m cadence (per-coin files; needs --coins)."),
        "snapshots": DataType("snapshots", "parquet", False, "reservoir_snapshots", _snapshots_key,
                              note="Daily account snapshots (positions, balances, account value)."),
    },
)
