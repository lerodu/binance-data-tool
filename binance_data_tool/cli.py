"""`binance-data` console tool. Currently covers Binance spot candles.

    binance-data download spot candles --all --interval 1m --workers 16
    binance-data download spot candles --start-year 2020 --end-year 2025 --interval 1h
    binance-data consolidate spot candles --all --interval 1m
"""

import argparse
from datetime import date

from . import consolidate, download

FIRST_YEAR = 2017  # Binance spot launched 2017


def _add_year_args(p):
    # time range is required: exactly one of --all / --year / (--start-year + --end-year)
    p.add_argument("--all", action="store_true",
                   help=f"everything: {FIRST_YEAR} through the current year")
    p.add_argument("--year", type=int, help="single year")
    p.add_argument("--start-year", type=int, help="first year (with --end-year)")
    p.add_argument("--end-year", type=int, help="last year (with --start-year)")
    p.add_argument("--interval", required=True, help="bar length, e.g. 1m, 5m, 1h, 1d, 1w")
    p.add_argument("--cache", default="vision_cache", help="download cache directory")


def _years(args, parser):
    if args.all:
        if args.year or args.start_year or args.end_year:
            parser.error("--all cannot be combined with --year/--start-year/--end-year")
        return list(range(FIRST_YEAR, date.today().year + 1))
    if args.year is not None:
        return [args.year]
    if args.start_year is not None and args.end_year is not None:
        return list(range(args.start_year, args.end_year + 1))
    parser.error("specify a time range: --all, --year Y, or --start-year X --end-year Y")


def _download_opts(p):
    p.add_argument("--workers", type=int, default=12)
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
    years = _years(args, ap)

    if args.action == "download":
        download.run(years, cache=args.cache, interval=args.interval, workers=args.workers,
                     symbols=args.symbols, recheck_missing=args.recheck_missing)
    elif args.action == "consolidate":
        consolidate.run(years, cache=args.cache, interval=args.interval,
                        out_prefix=args.out_prefix)


if __name__ == "__main__":
    main()
