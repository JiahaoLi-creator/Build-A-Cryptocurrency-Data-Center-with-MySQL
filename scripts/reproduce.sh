#!/usr/bin/env bash
# Replay the market-data examples against an isolated MySQL server.
set -euo pipefail
project=$(cd "$(dirname "$0")/.." && pwd)
python_bin=${PYTHON:-python3.11}
mode=${1:-}
case "$mode" in
  ""|--offline|--archives|--outage|--suspension) ;;
  *) echo "Usage: bash scripts/reproduce.sh [--offline|--archives|--outage|--suspension]" >&2; exit 2 ;;
esac
for bin in "$python_bin" mysqld mysql mysqladmin; do
  command -v "$bin" >/dev/null || { echo "Missing executable: $bin" >&2; exit 1; }
done
"$python_bin" -c 'import sys; sys.exit(0 if sys.version_info[:2] == (3, 11) else "Use Python 3.11 or set PYTHON to its path")'
work=$(mktemp -d /tmp/mysql-kline.XXXXXXXX)
socket="$work/mysql.sock"
pid=
admin=(mysqladmin --no-defaults --protocol=socket --socket="$socket" -u root)
cleanup() {
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    "${admin[@]}" shutdown >/dev/null 2>&1 || kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -rf -- "$work"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if [[ "$mode" != --offline ]]; then
  "$python_bin" -m venv "$work/venv"
  python_bin="$work/venv/bin/python"
  "$python_bin" -m pip --disable-pip-version-check install --quiet -r "$project/requirements.txt"
fi
"$python_bin" - "$project/requirements.txt" <<'PY'
from importlib.metadata import version
from pathlib import Path
import sys

for line in Path(sys.argv[1]).read_text().splitlines():
    name, expected = line.split("==")
    if version(name) != expected:
        raise SystemExit(f"Install requirements.txt: {name} should be {expected}")
PY
"$python_bin" -m pip check
basedir=$("$python_bin" -c 'import pathlib,shutil; print(pathlib.Path(shutil.which("mysqld")).resolve().parent.parent)')
options=(--no-defaults --basedir="$basedir" --datadir="$work/data" --user="$(id -un)"
         --innodb-buffer-pool-size=64M --log-error="$work/mysql.log")
if [[ -f "$basedir/lib/plugin/component_reference_cache.so" ]]; then
  options+=(--plugin-dir="$basedir/lib/plugin")
fi
if ! mysqld "${options[@]}" --initialize-insecure; then
  cat "$work/mysql.log" >&2
  exit 1
fi
mysqld "${options[@]}" --socket="$socket" --pid-file="$work/mysql.pid" --skip-networking --mysqlx=OFF &
pid=$!
for ((n=0; n<60; n++)); do
  if "${admin[@]}" ping --silent >/dev/null 2>&1; then break; fi
  if ! kill -0 "$pid" 2>/dev/null; then cat "$work/mysql.log" >&2; exit 1; fi
  sleep 1
done
if ! "${admin[@]}" ping --silent >/dev/null; then cat "$work/mysql.log" >&2; exit 1; fi
client=(mysql --no-defaults --protocol=socket --socket="$socket" -u root)
"${client[@]}" -e 'CREATE DATABASE binance_utc CHARACTER SET utf8mb4; SELECT VERSION() AS mysql_version;'
"${client[@]}" binance_utc < "$project/schema.sql"
export MYSQL_UNIX_PORT="$socket" MYSQL_HOST=localhost MYSQL_PORT=3306 MYSQL_USER=root MYSQL_PASSWORD= MYSQL_DATABASE=binance_utc
cd "$project"
"$python_bin" -m unittest discover -s tests -v
"$python_bin" demo.py

if [[ "$mode" == --outage || "$mode" == --suspension ]]; then
  example=${mode#--}
  database=${example}_replay
  "${client[@]}" -e "CREATE DATABASE $database CHARACTER SET utf8mb4"
  "${client[@]}" "$database" < "$project/schema.sql"
  MYSQL_DATABASE="$database" "$python_bin" "${example}_demo.py"
fi

if [[ "$mode" == --archives ]]; then
  mkdir "$work/archives"
  for day in 2024-12-31 2025-01-01; do
    file="BTCUSDT-1m-$day.zip"
    base="https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1m"
    for name in "$file" "$file.CHECKSUM"; do
      curl --fail --silent --show-error --location --retry 3 --max-time 60 "$base/$name" -o "$work/archives/$name"
    done
  done
  range=(--symbol BTCUSDT --start 2024-12-31 --end 2025-01-02)
  snapshot=("${range[@]}" --archive-dir "$work/archives")
  for pass in 1 2; do
    "$python_bin" archive.py import "${snapshot[@]}"
    counts=$("${client[@]}" --batch --skip-column-names binance_utc -e \
      'SELECT COUNT(*), COUNT(DISTINCT symbol,time_interval,candle_begin_time) FROM k')
    if [[ "$counts" != $'2880\t2880' ]]; then
      echo "Unexpected total and distinct candle counts on pass $pass: $counts" >&2
      exit 1
    fi
    echo "Archive import $pass: 2880 rows, 2880 unique candle keys."
  done
  "$python_bin" archive.py reconcile "${snapshot[@]}"

  # The changed candle lives in a separate schema on this disposable server.
  "${client[@]}" <<'SQL'
CREATE DATABASE archive_test;
CREATE TABLE archive_test.k LIKE binance_utc.k;
INSERT INTO archive_test.k SELECT * FROM binance_utc.k;
UPDATE archive_test.k SET volume = volume + 1
WHERE symbol = 'BTCUSDT' AND time_interval = '1m' AND candle_begin_time = '2025-01-01 00:00:00';
SQL
  status=0
  MYSQL_DATABASE=archive_test "$python_bin" archive.py reconcile "${snapshot[@]}" > "$work/diff.csv" || status=$?
  if [[ "$status" != 1 ]]; then echo "Expected a reconciliation difference, got exit $status" >&2; exit 1; fi
  cat "$work/diff.csv"
  "$python_bin" - "$work/diff.csv" <<'PY'
import csv
import sys

with open(sys.argv[1], newline="") as report:
    rows = list(csv.DictReader(report))
expected = dict(issue="field_mismatch", symbol="BTCUSDT", interval="1m", open_time_utc="2025-01-01T00:00:00Z",
                field="volume", archive_value="8.21827", mysql_value="9.21827", delta="1")
if rows != [expected]:
    raise SystemExit(f"Unexpected reconciliation output: {rows}")
PY
  echo "Verified one changed volume in the test copy. Rechecking the original database against the online archives."
  "$python_bin" archive.py reconcile "${range[@]}"
fi
