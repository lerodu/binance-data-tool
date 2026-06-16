"""Source / datatype descriptors.

A `Source` is a data *provider* (a bucket + auth + key layout), e.g.
`hydromancer-reservoir` or `hyperliquid-archive`. Each exposes one or more
`DataType`s. The downloader and ingester are source-agnostic: they ask the
descriptor for S3 keys and the destination table name. No cross-source column
harmonization happens here — each (source, datatype) lands in its own table with
that source's native columns.
"""

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Callable

from . import s3


def dates_in_range(start, end):
    """Inclusive list of 'YYYY-MM-DD' strings from start to end (date or str)."""
    def d(x):
        return x if isinstance(x, date) else date.fromisoformat(x)
    s, e = d(start), d(end)
    out, cur = [], s
    while cur <= e:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


@dataclass(frozen=True)
class DataType:
    name: str                                   # candles | fills | orderbook | snapshots | asset-ctxs
    fmt: str                                     # "parquet" | "lz4"
    per_coin: bool                               # one file per coin (orderbook) vs all coins/day
    table: str                                   # destination SQL table (source-prefixed)
    key: Callable[[str, str, str, str], str]     # (dex, asset_class, date, coin) -> S3 key
    native_interval: str = ""                    # e.g. "1s" for candles
    note: str = ""                               # human description (shown by `mktdata info`)


@dataclass(frozen=True)
class Source:
    name: str                                    # CLI token, e.g. "hydromancer-reservoir"
    bucket: str
    region: str
    requester_pays: bool
    signed: bool
    by_dex: bool                                 # whether --dex applies / dex discovery is possible
    datatypes: dict                              # name -> DataType
    dex_prefix: str = "by_dex/"                  # prefix under which dex sub-prefixes live
    note: str = ""                               # human description (shown by `mktdata info`)

    def s3(self):
        return s3.client(self.region, self.signed)

    def get(self, key):
        return s3.get_bytes(self.s3(), self.bucket, key, self.requester_pays)

    def discover_dexes(self):
        """List dex names from the bucket's by_dex/ prefixes (best effort)."""
        if not self.by_dex:
            return []
        prefixes = s3.list_prefixes(self.s3(), self.bucket, self.dex_prefix, self.requester_pays)
        return sorted(p[len(self.dex_prefix):].rstrip("/") for p in prefixes)

    def coins_for(self, dt: DataType, dex, asset_class, day):
        """For per-coin datatypes (orderbook): list available coins on a day by
        listing the date= partition and stripping the .parquet filename."""
        # key for a placeholder coin gives us the date-partition prefix
        sample = dt.key(dex, asset_class, day, "__")
        prefix = sample.rsplit("/", 1)[0] + "/"
        keys = s3.list_keys(self.s3(), self.bucket, prefix, self.requester_pays)
        return sorted(k.rsplit("/", 1)[-1].rsplit(".", 1)[0] for k in keys if k.endswith(".parquet"))
