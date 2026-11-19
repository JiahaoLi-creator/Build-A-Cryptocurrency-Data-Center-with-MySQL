# Binance Spot Market Data Center

This project stores closed, one-minute Binance Spot candles in MySQL so multiple strategies can read the same market history. A WebSocket collector writes each candle once, and a REST job checks a selected period for missing minutes and backfills them. The reader builds open, high, low, close and volume (OHLCV) bars only when every minute in the requested interval is present. All candle times are in UTC.

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

## Run against a persistent MySQL database

Create a UTC database and a local application account as a MySQL administrator. MySQL returns the generated password for use in the local configuration. For a Unix socket connection, use `localhost` as the account host.

```sql
CREATE DATABASE IF NOT EXISTS binance_utc CHARACTER SET utf8mb4;
CREATE USER 'kline'@'127.0.0.1' IDENTIFIED BY RANDOM PASSWORD;
GRANT SELECT, INSERT, UPDATE, CREATE TEMPORARY TABLES
    ON binance_utc.* TO 'kline'@'127.0.0.1';
```

Create the candle table, install the Python dependencies and copy the configuration template.

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

### Collect, repair and read

Run the three commands in order. The collector exits after storing one closed candle. The repair job checks the last 15 closed minutes for each configured symbol. The reader then reports the complete bars in the last two five-minute windows.

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

To inspect the most recent one-minute candles directly in MySQL, run this query.

```sql
SELECT symbol, candle_begin_time, open, high, low, close, volume
FROM k WHERE symbol = 'BTCUSDT' AND time_interval = '1m'
ORDER BY candle_begin_time DESC LIMIT 10;
```

## Data handling

- **Duplicate events.** Table `k` enforces a unique key on `(symbol, time_interval, candle_begin_time)`. The first stored candle is retained when a WebSocket event or REST response is repeated. Prices and quantities use `DECIMAL`. The optional `candles_utc.csv` records arrivals and can contain repeated events. Set `CSV_FILE=` to disable that log.
- **Missing minutes.** The repair job checks every minute from the inclusive start to the exclusive end, including gaps at either edge. Its default window is the last 100 closed minutes. Use `--minutes` and `--end` to select another range. Each REST request covers at most 1,000 consecutive missing minutes. If gaps remain, a one-time run reports the count and exits with a nonzero status. [Binance candlestick endpoint](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints)
- **Complete bars.** Windows align to UTC boundaries. The reader uses the first open, highest high, lowest low, last close and total volume. A five-minute bar requires five distinct closed minutes. During a live read, the reader waits up to about five seconds for the latest closed minute. If it is still missing, the reader skips that symbol. It reports how many requested windows are complete.
- **Existing data.** The default database is `binance_utc`. When importing an existing `binance.k` table, identify the source time zone, convert open times to UTC and reconcile duplicate candle keys before loading the data.
