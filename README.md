# mktdata

Acquire exchange market data: historical candles from the data.binance.vision S3 archive, and live order-book capture over websockets.

Command grammar: `mktdata <action> <exchange> <market> <datatype> [options]`.

```bash
uv sync
mktdata download binance spot candles --all --interval 1m --workers 16
mktdata consolidate binance spot candles --all --interval 1m
mktdata capture binance spot orderbook --symbols BTCUSDT,ETHUSDT --duration 1h
```

(Run via `uv run mktdata ...` inside the project.)

# Commands

## download binance spot candles

Downloads the monthly kline zips for every spot symbol into a local cache. A time range (`--all`, `--year`, or `--start-year` + `--end-year`) and `--interval` are required.

- Discovers all spot symbols including delisted ones by paginating the S3 bucket listing.
- Verifies each zip (CRC plus SHA256 against the published `.CHECKSUM`) before renaming it into the cache, so a cached file is always correct.
- Resumes by skipping already-cached files instantly without re-hashing, and retries transient network errors.
- Marks months a symbol was not listed as `.missing` (HTTP 404) and skips them on later runs; `--recheck-missing` re-attempts them.

| Option | Default | Description |
|---|---|---|
| `--all` | none | All years from 2017 through the current year (time range required: this or the ones below). |
| `--year Y` | none | Single year. |
| `--start-year Y` / `--end-year Y` | none | Inclusive year range (use both together). |
| `--interval S` | required | Bar length: `1s`..`1d`, `1w` (e.g. `1m`, `5m`, `1h`). `1M` unsupported (irregular). |
| `--cache DIR` | `vision_cache` | Directory the zips are downloaded into. |
| `--workers N` | `12` | Parallel download threads. |
| `--symbols LIST` | discover all | Comma list to download instead of scanning the bucket, e.g. `BTCUSDT,ETHUSDT`. |
| `--recheck-missing` | off | Clear `.missing` markers and re-attempt months previously seen as 404. |

## consolidate binance spot candles

Builds one aligned `.npy` per year from the cached zips. Same time-range and `--interval` requirements as download.

- Includes only the symbols that have data in that year.
- Places each candle at row index `(open_time - year_start) / interval`, leaving missing bars as `NaN`.
- Normalizes each row's timestamp to milliseconds (the source unit, s/ms/us/ns, can vary within a file) before placement.
- Writes `.meta.json` last as a completion marker and reports any zip that failed to parse as a data gap.

| Option | Default | Description |
|---|---|---|
| `--all` / `--year` / `--start-year` / `--end-year` | required | Which years to consolidate (one of these). |
| `--interval S` | required | Bar length; must match what was downloaded. |
| `--cache DIR` | `vision_cache` | Directory to read the zips from. |
| `--out-prefix P` | `klines` | Output files are `P_<year>.npy` + `P_<year>.meta.json`. |

Output per year: `klines_<year>.npy` shape `(n_symbols, bars, 9)` float32 (NaN where missing) plus `klines_<year>.meta.json` (symbol order, columns, interval, completion marker).
Columns: open, high, low, close, volume, quote_vol, trades, taker_buy_base, taker_buy_quote.

## capture binance spot orderbook

Live-captures the order book to a raw event log (order book has no archive, so it must be captured live). Writes every websocket message verbatim; the offline replay into arrays lives in the research repo, not here.

- Records depth diffs (`@depth@100ms`) + periodic REST snapshots + `@aggTrade`, time-ordered in one gzipped JSONL log.
- Maintains the book via Binance's local-book procedure with update-id gap detection and per-symbol resync (the new snapshot is logged so replay can rebuild the book).
- On connection loss the capture stops rather than reconnecting, so a log never contains an unmarked gap; restart to resume in a fresh file.
- Rotates the log file on a time interval so a long run is a sequence of complete files, not one monolith.

| Option | Default | Description |
|---|---|---|
| `--symbols LIST` | required | Comma list to capture, e.g. `BTCUSDT,ETHUSDT`. |
| `--duration D` | until Ctrl-C | Run length, e.g. `1h`, `30m`, `3600`. |
| `--out DIR` | `lob_capture` | Output directory for the raw log files. |
| `--rotate N` | `60` | Rotate the log file every N minutes (`0` = never). |

Record line: `{"t": <recv unix>, "v": "binance", "m": <message>[, "k": "rest_snapshot", "sym": ...]}`.
