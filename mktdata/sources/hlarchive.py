"""Official Hyperliquid archive source descriptor.

s3://hyperliquid-archive (ap-northeast-1, requester-pays as of 2025; the bucket
rejects anonymous access). Serves funding/OI/mark via asset_ctxs and L2 via
market_data. NO candles. Files are lz4-compressed (asset_ctxs as CSV, market_data
as line-delimited JSON per the HL docs).

NOTE: the exact key layout and file contents must be confirmed against the live
bucket (no creds were available at build time). Keys below follow the documented
`asset_ctxs/{YYYYMMDD}.csv.lz4` / `market_data/{YYYYMMDD}/{hour}/l2Book/{coin}.lz4`
patterns; adjust in one place here if the live bucket differs.
"""

from .base import DataType, Source

BUCKET = "hyperliquid-archive"
REGION = "ap-northeast-1"
FIRST_DATE = "2023-06-01"


def _ymd(day):                       # 'YYYY-MM-DD' -> 'YYYYMMDD'
    return day.replace("-", "")


def _asset_ctxs_key(dex, asset_class, day, coin):
    return f"asset_ctxs/{_ymd(day)}.csv.lz4"


def _l2_key(dex, asset_class, day, coin):
    # market_data is partitioned by hour; the downloader expands hours separately.
    # `coin` here carries 'HH/<COIN>' when hour-expanded (see download._archive_tasks).
    return f"market_data/{_ymd(day)}/{coin}.lz4"


SOURCE = Source(
    name="hyperliquid-archive",
    bucket=BUCKET,
    region=REGION,
    requester_pays=True,
    signed=True,
    by_dex=False,
    note="Official Hyperliquid archive (requester-pays). Source of funding/OI/mark. NO candles.",
    datatypes={
        "asset-ctxs": DataType("asset-ctxs", "lz4", False, "hlarchive_asset_ctxs", _asset_ctxs_key,
                               note="FUNDING RATE, open interest, mark & oracle price (daily csv.lz4)."),
        "orderbook":  DataType("orderbook",  "lz4", True,  "hlarchive_orderbook",  _l2_key,
                               note="L2 book snapshots (market_data, lz4; per-coin/hour)."),
    },
)
