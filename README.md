# Binance Spot K 线数据中心 · MySQL

多个策略可以共用一份行情。采集程序将 Binance 现货已收盘的 1 分钟 K 线写进 MySQL，补数程序用公开 REST 接口修复缺失的分钟，读取程序按指定周期输出完整的 OHLCV。所有时间都是 UTC。

本项目经朱建廷本人同意，根据其 2021 年 6 月 24 日的《MySQL搭建数字货币简易数据中心》整理。原教程提供了采集、补数与重采样的实现。本版增加了可重复运行的建库流程、数据库唯一键、统一时间处理、完整窗口检查和固定样例。[QuantClass 原帖](https://bbs.quantclass.cn/thread/5004) · [原脚本快照](https://github.com/JiahaoLi-creator/Build-A-Cryptocurrency-Data-Center-with-MySQL/tree/5713b3be9321cfc93ec96a41a847fa3071861994)

## 快速复现 · Quick start

准备 Python 3.11 和 MySQL 8.4 Server。终端需要能找到 `mysqld`、`mysql`、`mysqladmin` 和 Bash。验证环境为 macOS arm64、Python 3.11.14 和 MySQL 8.4.0。使用其他解释器路径时可指定 `PYTHON=/path/to/python3.11`。[Python 3.11](https://www.python.org/downloads/release/python-31114/) · [MySQL 8.4 安装文档](https://dev.mysql.com/doc/refman/8.4/en/installing.html)

```bash
git clone https://github.com/JiahaoLi-creator/Build-A-Cryptocurrency-Data-Center-with-MySQL.git
cd Build-A-Cryptocurrency-Data-Center-with-MySQL
bash scripts/reproduce.sh
```

The command installs the pinned Python packages, starts a temporary socket-only MySQL server, creates the schema and runs the replay twice. It cleans up its own database on exit. The market inputs are synthetic and bundled with the repository. Only the initial package installation needs internet access.

若想在断网时复现，可以提前安装依赖。`--offline` 使用指定的现有 Python 环境。

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
PYTHON="$PWD/.venv/bin/python" bash scripts/reproduce.sh --offline
```

### 样例如何证明补数、去重和聚合正确

样例给出 `BTCUSDT` 在 2024-01-01 00:00 至 00:09 UTC 的 10 根模拟分钟线。回放使用与实时程序相同的解析、写库和读取函数，只把交易所的网络响应换成固定输入。[分钟样例](examples/minutes.csv) · [预期五分钟结果](examples/expected_5m.csv) · [验收程序](demo.py)

1. 从采集事件中拿掉 00:03，重复发送 00:05，并先发送一条尚未收盘的 00:09。收到收盘事件后库中有 9 条唯一记录，只有 00:05 开始的五分钟窗口完整。
2. 先让模拟 REST 返回空结果，核实缺口仍可见。随后返回 00:03，核实只请求这一个分钟，库中恰好有 10 条记录，逐根分钟线都与样例相符。
3. 对同一张表再回放一遍。行数仍为 10，累计成功补数请求仍为 1，两根聚合结果不变。程序同时检查最新分钟是否到齐，以及 `limit=1` 时返回的窗口。

| 开盘时间 UTC | Open | High | Low | Close | Volume |
|---|---:|---:|---:|---:|---:|
| 2024-01-01 00:00 | 100 | 108 | 98 | 107 | 15 |
| 2024-01-01 00:05 | 107 | 113 | 104 | 109 | 40 |

## 在自己的 MySQL 中运行

**建立一个新的 UTC 数据库。** 用数据库管理员在 MySQL 客户端执行以下 SQL。示例用户通过本机 TCP 连接。`IDENTIFIED BY RANDOM PASSWORD` 会返回生成的密码供本地配置使用。使用 Unix socket 时可将账户主机设为 `localhost`。

```sql
CREATE DATABASE IF NOT EXISTS binance_utc CHARACTER SET utf8mb4;
CREATE USER 'kline'@'127.0.0.1' IDENTIFIED BY RANDOM PASSWORD;
GRANT SELECT, INSERT, UPDATE, CREATE TEMPORARY TABLES
    ON binance_utc.* TO 'kline'@'127.0.0.1';
```

```bash
mysql -u root -p binance_utc < schema.sql
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
```

在 `.env` 中填写 `MYSQL_PASSWORD` 和要跟踪的 `SYMBOLS`。这是 shell 配置文件，特殊字符需要按 shell 的规则引用。每个运行程序的终端都执行以下三行来加载配置。示例配置采用 `BTCUSDT,ETHUSDT`，三个脚本共用同一清单。[配置模板](.env.example) · [表结构](schema.sql)

```bash
set -a
. ./.env
set +a
```

可先执行 `python demo.py` 验证连接和表结构。样例使用仅在该连接中可见的临时表。实时采集和 REST 补数只使用 Binance 公开现货行情接口，无需交易所账户或 API key。[Binance 行情接口说明](https://developers.binance.com/docs/binance-spot-api-docs/faqs/market_data_only)

**依次验证三个入口。** 第一条命令等待一根收盘分钟线写入，第二条检查最近 15 个已收盘分钟并补齐缺口，第三条输出最近两个完整的五分钟窗口。省略 `--once` 后，三个脚本可分别在独立终端持续运行。持续模式下补数任务每 30 分钟检查一次，读取任务按目标周期运行。

```bash
python 1_binance_data_to_mysql.py --once
python 2_check_data.py --once --minutes 15
python 3_get_data_and_sample.py --once --interval 5m --limit 2
```

需要核对同一个历史窗口时，可以固定右端点。例如下面两条命令覆盖 `[2024-01-01 00:00, 00:10) UTC`，使用 Binance 的真实历史数据。上面的固定样例使用独立的模拟数值。

```bash
SYMBOLS=BTCUSDT python 2_check_data.py --once --minutes 10 --end 2024-01-01T00:10:00Z
SYMBOLS=BTCUSDT python 3_get_data_and_sample.py --once --interval 5m --limit 2 --as-of 2024-01-01T00:10:00Z
```

可在 MySQL 客户端检查最近的分钟线。

```sql
SELECT symbol, candle_begin_time, open, high, low, close, volume
FROM k WHERE symbol = 'BTCUSDT' AND time_interval = '1m'
ORDER BY candle_begin_time DESC LIMIT 10;
```

## 数据与运行约定

- 表 `k` 按 `(symbol, time_interval, candle_begin_time)` 唯一。WebSocket 重发、REST 补数与并发写入同一分钟时保留第一条已落库记录。价格和数量使用 `DECIMAL`。可选的 `candles_utc.csv` 是采集事件日志，可能包含重发事件。配置 `CSV_FILE=` 可关闭它。
- 补数窗口是左闭右开的 UTC 分钟区间，包括缺失的首尾分钟。默认检查最近 100 分钟，可通过 `--minutes` 和 `--end` 指定更早的区间。连续缺口按每个请求最多 1000 根拆分。接口无数据时会报告剩余缺口，单次运行以非零状态退出。[Binance K 线接口](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints)
- 聚合按 UTC 边界对齐。五分钟窗口要求五个不同的、已收盘的连续分钟。开盘价取第一笔分钟开盘，最高价取最大值，最低价取最小值，收盘价取最后一分钟收盘，成交量求和。最新分钟迟到时会等待最多约 5 秒，仍缺失则跳过该币种。历史窗口内有缺口时只展示完整窗口，并报告实际数量。
- 旧教程的 `binance.k` 可能使用服务器本地时间并含重复行。本版默认使用新库 `binance_utc`。若要迁移旧数据，先根据原服务器时区转换时间，再核对重复记录后导入新表。
