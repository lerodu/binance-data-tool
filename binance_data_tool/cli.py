"""`binance-data` console tool. Currently covers Binance spot candles.

    binance-data download spot candles --all --workers 16
    binance-data download spot candles --start-year 2020 --end-year 2025 --quote USDT
    binance-data consolidate spot candles --all
"""

import argparse
from datetime import date

from . import consolidate, download

FIRST_YEAR = 2017  # Binance spot launched 2017


def _add_year_args(p):
    p.add_argument("--all", action="store_true",
                   help=f"everything: {FIRST_YEAR} through the current year")
    p.add_argument("--year", type=int, help="single year (shortcut for equal start/end)")
    p.add_argument("--start-year", type=int, default=FIRST_YEAR)
    p.add_argument("--end-year", type=int, default=date.today().year)
    p.add_argument("--interval", default="1m")
    p.add_argument("--cache", default="vision_cache", help="download cache directory")


def _years(args):
    if args.all:
        return list(range(FIRST_YEAR, date.today().year + 1))
    if args.year is not None:
        return [args.year]
    return list(range(args.start_year, args.end_year + 1))


def _download_opts(p):
    p.add_argument("--workers", type=int, default=12)
    p.add_argument("--quote", default=None, help="filter base quote, e.g. USDT")
    p.add_argument("--symbols", default=None, help="comma list to override discovery")
    p.add_argument("--recheck-missing", action="store_true",
                   help="clear .missing markers and re-attempt those months")


def _consolidate_opts(p):
    p.add_argument("--out-prefix", default="klines", help="output filename prefix")


def _spot_candles_leaf(action_parser, opts_fn, leaf_help):
    """action -> spot -> candles; attaches year args + action-specific opts."""
    market = action_parser.add_subparsers(dest="market", required=True)
    spot = market.add_parser("spot", help="spot market")
    datatype = spot.add_subparsers(dest="datatype", required=True)
    leaf = datatype.add_parser("candles", help=leaf_help)
    _add_year_args(leaf)
    opts_fn(leaf)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="binance-data",
                                 description="Fetch Binance market data from data.binance.vision.")
    actions = ap.add_subparsers(dest="action", required=True)

    dl = actions.add_parser("download", help="fetch raw data zips into the cache")
    _spot_candles_leaf(dl, _download_opts, "download spot OHLCV candles (klines)")

    co = actions.add_parser("consolidate", help="build one .npy per year from the cache")
    _spot_candles_leaf(co, _consolidate_opts, "consolidate cached spot candles")

    args = ap.parse_args(argv)
    years = _years(args)

    if args.action == "download":
        download.run(years, cache=args.cache, interval=args.interval, workers=args.workers,
                     quote=args.quote, symbols=args.symbols,
                     recheck_missing=args.recheck_missing)
    elif args.action == "consolidate":
        consolidate.run(years, cache=args.cache, interval=args.interval,
                        out_prefix=args.out_prefix)


if __name__ == "__main__":
    main()
