# binance-data-tool

Fetches data from binance API and S3 archive and consolidates them into .npy files

```bash
uv sync
uv run binance-data download spot candles --all --workers 16
uv run binance-data consolidate spot candles --all
```

# Commands

## download spot candles

Downloads the monthly kline zips for every spot symbol into a local cache.

- Discovers all spot symbols including delisted ones by paginating the S3 bucket listing.
- Verifies each zip (CRC plus SHA256 against the published `.CHECKSUM`) before renaming it into the cache, so a cached file is always correct.
- Resumes by skipping already-cached files instantly without re-hashing, and retries transient network errors.
- Marks months a symbol was not listed as `.missing` (HTTP 404) and skips them on later runs; `--recheck-missing` re-attempts them.

| Option | Default | Description |
|---|---|---|
| `--all` | off | All years from 2017 through the current year. |
| `--year Y` | none | Single year (shortcut for `--start-year Y --end-year Y`). |
| `--start-year Y` | `2017` | First year (inclusive). |
| `--end-year Y` | current year | Last year (inclusive). |
| `--interval S` | `1m` | Bar length: `1s`..`1d`, `1w` (e.g. `1m`, `5m`, `1h`). `1M` unsupported (irregular). |
| `--cache DIR` | `vision_cache` | Directory the zips are downloaded into. |
| `--workers N` | `12` | Parallel download threads. |
| `--quote Q` | all | Only symbols ending in this quote asset, e.g. `USDT`. |
| `--symbols LIST` | discover all | Comma list to download instead of scanning the bucket, e.g. `BTCUSDT,ETHUSDT`. |
| `--recheck-missing` | off | Clear `.missing` markers and re-attempt months previously seen as 404. |

## consolidate spot candles

Builds one aligned `.npy` per year from the cached zips.

- Includes only the symbols that have data in that year.
- Places each candle at row index `(open_time - year_start) / interval`, leaving missing bars as `NaN`.
- Normalizes each row's timestamp to milliseconds (the source unit, s/ms/us/ns, can vary within a file) before placement.
- Writes `.meta.json` last as a completion marker and reports any zip that failed to parse as a data gap.

| Option | Default | Description |
|---|---|---|
| `--all` / `--year` / `--start-year` / `--end-year` | as above | Which years to consolidate. |
| `--interval S` | `1m` | Bar length; must match what was downloaded. |
| `--cache DIR` | `vision_cache` | Directory to read the zips from. |
| `--out-prefix P` | `klines` | Output files are `P_<year>.npy` + `P_<year>.meta.json`. |

Output per year: `klines_<year>.npy` shape `(n_symbols, bars, 9)` float32 (NaN where missing) plus `klines_<year>.meta.json` (symbol order, columns, interval, completion marker).
Columns: open, high, low, close, volume, quote_vol, trades, taker_buy_base, taker_buy_quote.
