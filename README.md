# Binance Spot Market Data Center

This project stores closed, one-minute Binance Spot candles in MySQL. The WebSocket collector recovers missed trading minutes over REST after a restart or reconnection. A trading calendar excludes exchange-confirmed suspensions from backfill and aggregation. A historical importer loads verified daily archives into the same table. A reconciliation command compares stored prices, volumes and trade counts with the archive by UTC minute. The reader builds open, high, low, close and volume (OHLCV) bars from complete windows with a configurable UTC start minute.

## Run the sample

Install Python 3.11 and MySQL Server 8.4. Bash and the `mysqld`, `mysql` and `mysqladmin` executables must be on your `PATH`. Set `PYTHON` to the path of your Python 3.11 interpreter if needed. [Python 3.11](https://www.python.org/downloads/release/python-31114/) · [MySQL installation guide](https://dev.mysql.com/doc/refman/8.4/en/installing.html)

```bash
git clone https://github.com/JiahaoLi-creator/Build-A-Cryptocurrency-Data-Center-with-MySQL.git
cd Build-A-Cryptocurrency-Data-Center-with-MySQL
bash scripts/reproduce.sh
```

The script installs the pinned dependencies, starts a temporary MySQL server on a local socket, and runs the tests. It replays the bundled sample twice against the same table, then removes the temporary database. The replay uses synthetic data and needs no Binance account or API key.

The command passed with MySQL 8.4.0 on macOS arm64 and Debian 12 x86-64. On the tested Debian 12 ARM64 virtual machine, OpenSSL's CPU detection raised `SIGILL`. The replay passed there with `OPENSSL_armcap=0 bash scripts/reproduce.sh`.

For an offline run, install the dependencies in advance and point the script at that environment.

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
PYTHON="$PWD/.venv/bin/python" bash scripts/reproduce.sh --offline
```

### What the replay verifies

The sample contains ten synthetic `BTCUSDT` candles from 00:00 through 00:09 UTC on 2024-01-01. It exercises the same parsing, database writes and aggregation as the live pipeline. [Minute candles](examples/minutes.csv) · [Expected five-minute bars](examples/expected_5m.csv) · [Replay code](demo.py)

1. The collector misses 00:03, receives 00:05 twice, and sees an update for 00:09 before it closes. After the 00:09 close arrives, MySQL holds nine distinct minutes. Only the five-minute bar starting at 00:05 is complete.
2. An empty REST response leaves the gap visible. When the next response supplies 00:03, the repair job fills that minute and all ten stored candles match the sample.
3. Replaying the events against the same table leaves ten rows and triggers no further backfill. The reader returns the two bars below and returns only the second bar when `limit=1`.

| Open time (UTC) | Open | High | Low | Close | Volume |
|---|---:|---:|---:|---:|---:|
| 2024-01-01 00:00 | 100 | 108 | 98 | 107 | 15 |
| 2024-01-01 00:05 | 107 | 113 | 104 | 109 | 40 |

## Reproduce the two-day historical import

From the cloned repository, run the following command with the same prerequisites as the sample, plus `curl` and internet access. It starts a fresh MySQL server, runs the ten-minute sample, and downloads the two public BTCUSDT Spot archives and their SHA-256 manifests. [Linux verification runs](https://github.com/JiahaoLi-creator/Build-A-Cryptocurrency-Data-Center-with-MySQL/actions/workflows/archive-replay.yml)

```bash
bash scripts/reproduce.sh --archives
```

| UTC archive date | Timestamp unit in the ZIP | Distinct minutes |
|---|---|---:|
| 2024-12-31 | Milliseconds | 1,440 |
| 2025-01-01 | Microseconds | 1,440 |

The replay imports the same two files twice. After each pass, MySQL must report **2,880 rows and 2,880 distinct candle keys**. All nine stored market fields must match the archives. The replay then copies the table to a separate test database and increases the base-asset volume for the first minute of 2025 by exactly one BTC. The report must contain exactly the row below. A final comparison downloads the archives again and must find zero differences in the original database.

```csv
issue,symbol,interval,open_time_utc,field,archive_value,mysql_value,delta
field_mismatch,BTCUSDT,1m,2025-01-01T00:00:00Z,volume,8.21827,9.21827,1
```

## Reproduce a three-hour collector outage

From the cloned repository, run this command with the sample prerequisites and internet access. It starts an isolated MySQL database and downloads the checksum-verified BTCUSDT Spot archive for January 1, 2025.

```bash
bash scripts/reproduce.sh --outage
```

Three separate Python processes run the collector's normal `--once` entry point against the same database. The replay controls the clock and plays the two boundary candles as WebSocket events. The first process saves the 00:59 UTC close, then goes offline. The next process starts at 04:00 UTC and fetches **180 missing minutes**, from 01:00 through 03:59, from the public Binance Spot REST endpoint. The 04:00 candle arrives over the simulated socket before the REST rows are written. The third process restarts against the recovered database and receives the same 04:00 close again.

| Collector process | REST requests | Unique minutes in the outage | Total rows / unique keys |
|---|---:|---:|---:|
| Before the outage | 0 | 0 | 1 / 1 |
| First restart | 1 | 180 | 182 / 182 |
| Repeat restart | 0 | 180 | 182 / 182 |

The 182 rows include one live candle on each side of the outage. The replay compares all nine market fields with the archive and verifies that the next minute to check is 04:01 UTC. [Replay code](outage_demo.py) · [Linux verification runs](https://github.com/JiahaoLi-creator/Build-A-Cryptocurrency-Data-Center-with-MySQL/actions/workflows/archive-replay.yml)

## Reproduce a trading suspension and a missing minute

From the cloned repository, run the following command with the sample prerequisites and internet access.

```bash
bash scripts/reproduce.sh --suspension
```

Binance stopped VIDTUSDT Spot trading on October 31, 2022 at 03:00 UTC for a token swap. Trading reopened on November 9 at 08:00 UTC. The **13,260-minute** suspension includes the stop time and excludes the reopening time. The two checksum-verified daily archives place the last pre-suspension candle at 02:59 and the first relisted candle at 08:00. Prices on each side retain their original traded units. [Exchange announcement](https://www.binance.com/en/support/announcement/detail/ef40b40af1944bc9b399ad9c52861a04)

The replay uses the last five minutes before delisting and the first five after relisting. It deliberately omits **2022-11-09 08:02 UTC**. A bounded audit checks the entire suspension. The normal collector then starts with a simulated clock and a recorded WebSocket close. It fetches the omitted minute from the public Spot REST endpoint. The replay records the time bounds of every backfill HTTP request and compares all nine stored fields against the archives.

| Stage | Backfill requests | Requests touching the suspension | Unique minutes in MySQL | Raw rows in suspension | Complete five-minute bars |
|---|---:|---:|---:|---:|---:|
| Suspension-only audit | 0 | 0 | 9 | 0 | 1 |
| Automatic recovery | 1 | 0 | 10 | 0 | 2 |
| Repeat startup | 0 | 0 | 10 | 0 | 2 |

The only five-minute bars open at **October 31 02:55 UTC** and **November 9 08:00 UTC**. The checkpoint ends at November 9 08:05 UTC. [Replay code](suspension_demo.py) · [Linux verification runs](https://github.com/JiahaoLi-creator/Build-A-Cryptocurrency-Data-Center-with-MySQL/actions/workflows/archive-replay.yml)

## Reproduce five-minute bars with a shifted start

Run the following command from the cloned repository with the sample prerequisites and internet access.

```bash
bash scripts/reproduce.sh --offset
```

The replay verifies the SHA-256 checksum of the BTCUSDT Spot minute archive for January 1, 2025 and loads its first ten minutes into an isolated MySQL database. At the 00:10 UTC cutoff, the same ten records produce the following complete bars. Prices are in USDT and volume is in BTC. [Minute archive](https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2025-01-01.zip) · [Published five-minute candles](https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=5m&startTime=1735689600000&endTime=1735690199999&limit=2)

| Offset (min) | Minute opens included (UTC) | Open | High | Low | Close | Volume |
|---:|---|---:|---:|---:|---:|---:|
| 0 | 00:00 through 00:04 | 93,576.00 | 93,702.15 | 93,537.50 | 93,661.20 | 45.94160 |
| 0 | 00:05 through 00:09 | 93,661.20 | 93,678.02 | 93,600.00 | 93,607.10 | 71.94585 |
| 2 | 00:02 through 00:06 | 93,652.00 | 93,702.15 | 93,600.00 | 93,632.05 | 61.04142 |

The two default bars must match the exchange's published five-minute candles. For the shifted bar, the replay calculates all five OHLCV fields independently from the five archived minute rows and checks the first instant the result becomes available, 00:07 UTC. The preceding shifted window needs three minutes from the previous day. The following window closes at 00:12 UTC. Only the middle window qualifies in this sample. [Replay code](offset_demo.py)

## Run against a persistent MySQL database

Create a UTC database and a local application account as a MySQL administrator. MySQL returns the generated password for use in the local configuration. For a Unix socket connection, use `localhost` as the account host.

```sql
CREATE DATABASE IF NOT EXISTS binance_utc CHARACTER SET utf8mb4;
CREATE USER 'kline'@'127.0.0.1' IDENTIFIED BY RANDOM PASSWORD;
GRANT SELECT, INSERT, UPDATE, CREATE TEMPORARY TABLES
    ON binance_utc.* TO 'kline'@'127.0.0.1';
```

Create the candle and checkpoint tables, install the Python dependencies and copy the configuration template. For an existing installation, reapply `schema.sql` as the database administrator before restarting the collector. Both tables use `CREATE TABLE IF NOT EXISTS`.

```bash
mysql -u root -p binance_utc < schema.sql
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
```

Set `MYSQL_PASSWORD` and `SYMBOLS` in `.env`, quoting shell special characters as needed. The example tracks `BTCUSDT,ETHUSDT`. Load the same configuration in each terminal that runs a job. [Configuration template](.env.example) · [Table schema](schema.sql)

```bash
set -a
. ./.env
set +a
```

Run `python demo.py` to check the database connection and schema in a session-local temporary table. The live collector and backfill job use Binance's public market-data endpoints. [Binance market-data API](https://developers.binance.com/docs/binance-spot-api-docs/faqs/market_data_only)

### Import and reconcile historical candles

Both commands use one Spot symbol and a range of completed UTC days. The start is inclusive and the end is exclusive, so the example covers December 31 and January 1.

```bash
python archive.py import --symbol BTCUSDT --start 2024-12-31 --end 2025-01-02
python archive.py reconcile --symbol BTCUSDT --start 2024-12-31 --end 2025-01-02 > differences.csv
```

Each command downloads the official daily one-minute ZIP and its `.CHECKSUM` for dates with expected trading minutes. It verifies the SHA-256 digest, UTC date, timestamp unit, minute alignment and unique source keys before using a file. Valid partial trading days retain the minutes present in the archive. A fully suspended UTC day skips the download, and reconciliation reports any existing MySQL candle on that date as `unexpected_in_mysql`. The importer commits one day at a time. An interrupted range can be replayed safely, and a previously stored candle keeps its original values. To use a saved snapshot, pass `--archive-dir DIR` to either command and put the ZIP files and their matching `.zip.CHECKSUM` files together in that directory. [Binance public archive format](https://github.com/binance/binance-public-data#data-information)

Reconciliation reads MySQL without changing it. It compares open, high, low, close, base volume, quote volume, trade count, taker-buy base volume and taker-buy quote volume at each UTC open time. Prices and quantities use exact decimal arithmetic. The CSV contains one row per differing field, with `delta = mysql_value - archive_value`. A minute present on only one side produces a single `missing_in_mysql` or `unexpected_in_mysql` row. Progress, source hashes and totals go to standard error.

| Reconciliation exit code | Meaning |
|---:|---|
| 0 | Comparison completed with zero differences. The CSV contains only its header. |
| 1 | Comparison completed and found differences. |
| 2 | The command could not complete. Check the input, checksum, connection or database error before using the report. |

### Collect, repair and read

The collector automatically resumes each configured symbol and exits after one live close in `--once` mode. If every selected symbol is currently suspended, it checks earlier trading minutes and exits. The second command is a bounded audit of the last 15 closed minutes. The reader reports complete bars in the last two five-minute windows.

```bash
python 1_binance_data_to_mysql.py --once
python 2_check_data.py --once --minutes 15
python 3_get_data_and_sample.py --once --interval 5m --limit 2
```

For continuous operation, omit `--once` and run each job in its own terminal. The repair job runs every 30 minutes, and the reader runs at the requested interval.

To inspect the same historical period across runs, give both jobs a fixed UTC cutoff. These commands use public Binance history for the ten minutes starting at midnight on 2024-01-01.

```bash
SYMBOLS=BTCUSDT python 2_check_data.py --once --minutes 10 --end 2024-01-01T00:10:00Z
SYMBOLS=BTCUSDT python 3_get_data_and_sample.py --once --interval 5m --limit 2 --as-of 2024-01-01T00:10:00Z
```

Use `--offset-minutes` to shift bar opens forward from the UTC Unix epoch. The default is zero. The offset accepts whole minutes from zero up to the interval length, exclusive. For example, five-minute bars at offset two open at :02, :07, :12 and so on. The continuous reader wakes on that schedule. The Python reader accepts the same setting as `get_k_data(..., offset_minutes=2)`.

```bash
SYMBOLS=BTCUSDT python 3_get_data_and_sample.py --once --interval 5m --offset-minutes 2 --limit 1 --as-of 2025-01-01T00:10:00Z
```

`--limit` counts the most recent closed windows on the selected grid. Every constituent minute must be present and open for trading. A bar is labelled by its opening time and becomes available at its exclusive end. Offsets apply when reading and use the same stored one-minute records.

To inspect the most recent one-minute candles directly in MySQL, run this query.

```sql
SELECT symbol, candle_begin_time, open, high, low, close, volume
FROM k WHERE symbol = 'BTCUSDT' AND time_interval = '1m'
ORDER BY candle_begin_time DESC LIMIT 10;
```

### How restart recovery works

MySQL stores the next unverified UTC minute for each symbol in `collector_checkpoint`. On first startup, a symbol with existing candles begins verification at its oldest stored closed minute. An empty symbol starts at the current UTC minute. A live close advances the checkpoint only if it closes the next expected minute. A later close is stored immediately while the gap stays pending.

The collector checks for gaps on startup and when the socket reconnects. In continuous mode it also checks again after 60 seconds. A background worker verifies up to 1,000 closed minutes per symbol per turn and issues REST requests only for missing minutes. It advances the checkpoint through the contiguous rows confirmed in MySQL. An empty or partial response leaves the earliest unresolved minute pending for a retry. If the process stops after writing rows but before updating the checkpoint, the next run reads those rows and requests only what is still missing. A one-time run exits with a nonzero status if recovery is incomplete. The independent repair command's 100-minute default applies only to that command.

### Trading calendar

Each calendar entry identifies a Spot symbol, the first suspended UTC minute, the first minute open for trading again, the reason and an exchange source URL. An empty `resume_utc` marks an ongoing suspension. The collector can advance past a documented closure without inserting candles, and keeps its position at an ongoing closure until a reopening is recorded. To extend the calendar, add an exchange-confirmed interval and restart running jobs. Outside the listed intervals, an absent minute remains eligible for backfill. An exchange-reported zero-volume candle during trading is valid. [Calendar entries](data/spot_suspensions.csv)

## Data handling

- **Duplicate events.** Table `k` enforces a unique key on `(symbol, time_interval, candle_begin_time)`. WebSocket, REST and archive imports all retain the first stored candle for that key. Prices and quantities use `DECIMAL`. The optional `candles_utc.csv` records WebSocket arrivals and can contain repeated events. Set `CSV_FILE=` to disable that log.
- **Missing minutes.** The repair job checks expected trading minutes from the inclusive start to the exclusive end, including gaps at either edge. It reports missing and suspended minutes separately. Its default window is the last 100 closed minutes. Use `--minutes` and `--end` to select another range. Each REST request covers at most 1,000 consecutive missing trading minutes. If gaps remain, a one-time run reports the count and exits with a nonzero status. [Binance candlestick endpoint](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints)
- **Complete bars.** Windows align to the selected UTC offset. The reader uses the first open, highest high, lowest low, last close and total volume. A five-minute bar requires five distinct closed minutes. During a live read, the reader waits up to about five seconds for the latest closed minute. If it is still missing, the reader skips that symbol. It reports how many requested windows are complete.
- **Existing data.** The default database is `binance_utc`. When importing an existing `binance.k` table, identify the source time zone, convert open times to UTC and reconcile duplicate candle keys before loading the data.
