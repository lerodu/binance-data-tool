"""`mktdata` console tool. Grammar: <action> <exchange> <market> <datatype>.

    mktdata download binance spot candles --all --interval 1m --workers 16
    mktdata download binance um   candles --all --interval 1m   # USD-margined futures
    mktdata download binance cm   candles --all --interval 1m   # coin-margined futures
    mktdata consolidate binance um candles --all --interval 1m
    mktdata capture binance spot orderbook --symbols BTCUSDT,ETHUSDT --duration 1h
"""

import argparse
from datetime import date

from . import capture, consolidate, download
from .sources import SOURCES, get_source
from .sources import download as rdl
from .sources import ingest as ring
from .sources import materialize as rmat
from .sources.sink import make_sink

FIRST_YEAR = 2017          # Binance spot launched 2017
MARKETS = ("spot", "um", "cm")  # spot, USD-margined futures, coin-margined futures


def _csv(v):
    return [x.strip() for x in v.split(",")] if v else None


def _parse_dexes(arg, src):
    if not src.by_dex:
        return [""]
    if not arg:
        return ["hyperliquid"]
    if arg == "all":
        return sorted(set(src.discover_dexes()) | {"hyperliquid"})
    return _csv(arg)


def _print_info():
    """Show every external-archive source and the data types it carries."""
    print("External-archive sources (use: mktdata download <source> <datatype> ...)\n")
    for sname, src in SOURCES.items():
        auth = "requester-pays (AWS creds required)" if src.requester_pays else "anonymous"
        print(f"● {sname}")
        print(f"    bucket s3://{src.bucket}  ({src.region}, {auth})")
        if src.note:
            print(f"    {src.note}")
        for dt in src.datatypes.values():
            print(f"      - {dt.name:<11} {dt.note}")
        print()
    print("Binance (historical candles) — unchanged grammar:")
    print("    mktdata download binance {spot,um,cm} candles --all --interval 1m")
    print("    mktdata consolidate binance {spot,um,cm} candles --all --interval 1m  (-> .npy)\n")
    print("Funding rate / open interest / mark+oracle price: hyperliquid-archive asset-ctxs")
    print("  (Hydromancer Reservoir does NOT carry funding — candles/fills/orderbook/snapshots only)")


def _register_sources(ex, action):
    """Register source-first grammar: <source> <datatype> + flags, for the new
    external archives (hydromancer-reservoir, hyperliquid-archive). `ex` is an
    existing exchange-subparsers object (shared with binance under `download`)."""
    for sname, src in SOURCES.items():
        dts = ex.add_parser(sname).add_subparsers(dest="datatype", required=True)
        for dtname in src.datatypes:
            leaf = dts.add_parser(dtname)
            leaf.add_argument("--cache", default="hl_cache")
            if action == "download":
                leaf.add_argument("--dex", default=None, help="comma list or 'all' (default hyperliquid)")
                leaf.add_argument("--asset-class", default="perp")
                leaf.add_argument("--coins", default=None, help="comma list (per-coin types)")
                leaf.add_argument("--start", required=True, help="YYYY-MM-DD")
                leaf.add_argument("--end", required=True, help="YYYY-MM-DD")
                leaf.add_argument("--workers", type=int, default=8)
                leaf.add_argument("--recheck-missing", action="store_true")
            elif action == "consolidate":
                # one verb; output format is a --sink option (sql | csv | dense npy)
                leaf.add_argument("--sink", default="sqlite", choices=["sqlite", "csv", "npy"],
                                  help="destination: sqlite/csv (faithful store) or npy (dense research slice)")
                leaf.add_argument("--db", default="hl.sqlite", help="sqlite path (--sink sqlite)")
                leaf.add_argument("--out", default=None, help="csv dir or .npy path (--sink csv/npy)")
                leaf.add_argument("--force", action="store_true", help="re-ingest already-done files")
                # npy-slice options (only used by --sink npy)
                leaf.add_argument("--interval", default="1m", help="resample interval for --sink npy")
                leaf.add_argument("--dex", default=None, help="comma list (npy filter)")
                leaf.add_argument("--coins", default=None, help="comma list (npy filter)")
                leaf.add_argument("--start", default=None, help="YYYY-MM-DD (npy slice)")
                leaf.add_argument("--end", default=None, help="YYYY-MM-DD (npy slice)")


def _chain(action_parser, exchange, market, datatype, leaf_help):
    """Single action -> exchange -> market -> datatype path; returns the leaf."""
    ex = action_parser.add_subparsers(dest="exchange", required=True)
    mk = ex.add_parser(exchange).add_subparsers(dest="market", required=True)
    dt = mk.add_parser(market).add_subparsers(dest="datatype", required=True)
    return dt.add_parser(datatype, help=leaf_help)


def _binance_candles(ex, add_args):
    """binance -> {spot,um,cm} -> candles on an existing exchange-subparsers `ex`."""
    mk = ex.add_parser("binance").add_subparsers(dest="market", required=True)
    for m in MARKETS:
        leaf = mk.add_parser(m).add_subparsers(dest="datatype", required=True).add_parser("candles")
        add_args(leaf)


def _add_year_args(p):
    # time range is required: exactly one of --all / --year / (--start-year + --end-year)
    p.add_argument("--all", action="store_true",
                   help=f"everything: {FIRST_YEAR} through the current year")
    p.add_argument("--year", type=int, help="single year")
    p.add_argument("--start-year", type=int, help="first year (with --end-year)")
    p.add_argument("--end-year", type=int, help="last year (with --start-year)")
    p.add_argument("--interval", required=True, help="bar length, e.g. 1m, 5m, 1h, 1d, 1w")
    p.add_argument("--cache", default=None, help="cache dir (default: vision_cache[_<market>])")


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


def _default_cache(market):
    return "vision_cache" if market == "spot" else f"vision_cache_{market}"


def _default_prefix(market):
    return "klines" if market == "spot" else f"klines_{market}"


def main(argv=None):
    ap = argparse.ArgumentParser(prog="mktdata",
                                 description="Fetch Binance market data from data.binance.vision "
                                             "(candles: spot/um/cm) and live websockets (order book).")
    actions = ap.add_subparsers(dest="action", required=True)

    def _dl_args(leaf):
        _add_year_args(leaf)
        leaf.add_argument("--workers", type=int, default=12)
        leaf.add_argument("--symbols", default=None, help="comma list to override discovery")
        leaf.add_argument("--recheck-missing", action="store_true",
                          help="clear .missing markers and re-attempt those months")

    def _co_args(leaf):
        _add_year_args(leaf)
        leaf.add_argument("--out-prefix", default=None,
                          help="output filename prefix (default: klines[_<market>])")
        leaf.add_argument("--workers", type=int, default=1, help="parallel symbol-parsing processes")
        leaf.add_argument("--force", action="store_true", help="rebuild years already complete")

    # download: binance candle zips + external-archive sources (reservoir / hl-archive)
    dl_ex = actions.add_parser("download", help="fetch into the cache") \
        .add_subparsers(dest="exchange", required=True)
    _binance_candles(dl_ex, _dl_args)
    _register_sources(dl_ex, "download")
    # consolidate: build a store from the cache. binance -> .npy/year; sources -> --sink {sqlite,csv,npy}
    co_ex = actions.add_parser("consolidate", help="build a store from the cache (--sink for sources)") \
        .add_subparsers(dest="exchange", required=True)
    _binance_candles(co_ex, _co_args)
    _register_sources(co_ex, "consolidate")
    # info: show every source and the data types it carries
    actions.add_parser("info", help="list sources and available data types")

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

    if args.action == "info":
        _print_info()
        return

    # external-archive sources (source-first grammar): download / consolidate
    if getattr(args, "exchange", None) in SOURCES:
        src = get_source(args.exchange)
        if args.action == "download":
            rdl.run(src, args.datatype, dexes=_parse_dexes(args.dex, src),
                    asset_class=args.asset_class, start=args.start, end=args.end,
                    coins=_csv(args.coins), cache=args.cache, workers=args.workers,
                    recheck_missing=args.recheck_missing)
        elif args.action == "consolidate":
            if args.sink == "npy":
                if args.datatype != "candles":
                    ap.error("--sink npy is only for candles (dense OHLCV array)")
                if not (args.start and args.end):
                    ap.error("--sink npy requires --start and --end")
                rmat.from_cache(src.name, args.cache, dexes=_csv(args.dex), coins=_csv(args.coins),
                                start=args.start, end=args.end, interval=args.interval,
                                out=args.out or "hl_klines.npy")
            else:
                sink = make_sink(args.sink, db=args.db, out=args.out or "hl_dump")
                try:
                    ring.run(src, args.datatype, sink, cache=args.cache, force=args.force)
                finally:
                    sink.close()
        return

    years = _years(args, ap)
    cache = args.cache or _default_cache(args.market)
    if args.action == "download":
        download.run(years, cache=cache, market=args.market, interval=args.interval,
                     workers=args.workers, symbols=args.symbols,
                     recheck_missing=args.recheck_missing)
    elif args.action == "consolidate":
        out_prefix = args.out_prefix or _default_prefix(args.market)
        consolidate.run(years, cache=cache, interval=args.interval,
                        out_prefix=out_prefix, workers=args.workers, force=args.force)


if __name__ == "__main__":
    main()
