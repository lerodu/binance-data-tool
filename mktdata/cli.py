"""`mktdata` console tool. Grammar: <action> <exchange> <market> <datatype>.

    mktdata download binance spot candles --all --interval 1m --workers 16
    mktdata consolidate binance spot candles --all --interval 1m
    mktdata capture binance spot orderbook --symbols BTCUSDT,ETHUSDT --duration 1h
"""

import argparse
from datetime import date

from . import capture, consolidate, download

FIRST_YEAR = 2017  # Binance spot launched 2017


def _chain(action_parser, exchange, market, datatype, leaf_help):
    """Build action -> exchange -> market -> datatype and return the leaf parser."""
    ex = action_parser.add_subparsers(dest="exchange", required=True)
    mk = ex.add_parser(exchange).add_subparsers(dest="market", required=True)
    dt = mk.add_parser(market).add_subparsers(dest="datatype", required=True)
    return dt.add_parser(datatype, help=leaf_help)


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


def _duration_s(v):
    if not v:
        return None
    v = str(v).strip().lower()
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return int(v[:-1]) * units[v[-1]] if v[-1] in units else int(v)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="mktdata",
                                 description="Fetch Binance market data from data.binance.vision "
                                             "(candles) and live websockets (order book).")
    actions = ap.add_subparsers(dest="action", required=True)

    dl = _chain(actions.add_parser("download", help="fetch candle zips into the cache"),
                "binance", "spot", "candles", "download spot OHLCV candles")
    _add_year_args(dl)
    dl.add_argument("--workers", type=int, default=12)
    dl.add_argument("--symbols", default=None, help="comma list to override discovery")
    dl.add_argument("--recheck-missing", action="store_true",
                    help="clear .missing markers and re-attempt those months")

    co = _chain(actions.add_parser("consolidate", help="build one .npy per year from the cache"),
                "binance", "spot", "candles", "consolidate cached spot candles")
    _add_year_args(co)
    co.add_argument("--out-prefix", default="klines", help="output filename prefix")
    co.add_argument("--workers", type=int, default=1, help="parallel symbol-parsing processes")
    co.add_argument("--force", action="store_true", help="rebuild years already marked complete")

    cap = _chain(actions.add_parser("capture", help="live-capture market data to a raw log"),
                 "binance", "spot", "orderbook", "live-capture the spot order book + trades")
    cap.add_argument("--symbols", required=True, help="comma list, e.g. BTCUSDT,ETHUSDT")
    cap.add_argument("--duration", default=None, help="run length, e.g. 1h, 30m, 3600 (default: until Ctrl-C)")
    cap.add_argument("--out", default="lob_capture", help="output directory for the raw log")
    cap.add_argument("--rotate", type=int, default=60, help="rotate the log file every N minutes (0 = never)")

    args = ap.parse_args(argv)

    if args.action == "capture":
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        writer = capture.RawWriter(args.out, f"{args.exchange}_{args.market}_{args.datatype}",
                                   rotate_min=args.rotate)
        adapter = capture.EXCHANGES[args.exchange][args.market][args.datatype](symbols, writer)
        capture.run(adapter, writer, _duration_s(args.duration))
        return

    years = _years(args, ap)
    if args.action == "download":
        download.run(years, cache=args.cache, interval=args.interval, workers=args.workers,
                     symbols=args.symbols, recheck_missing=args.recheck_missing)
    elif args.action == "consolidate":
        consolidate.run(years, cache=args.cache, interval=args.interval,
                        out_prefix=args.out_prefix, workers=args.workers, force=args.force)


if __name__ == "__main__":
    main()
