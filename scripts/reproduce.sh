#!/usr/bin/env bash
# One isolated MySQL server, one Python environment, a fixed market-data replay.
set -euo pipefail
project=$(cd "$(dirname "$0")/.." && pwd)
python_bin=${PYTHON:-python3.11}
offline=false
case "${1:-}" in
  --offline) offline=true ;;
  "") ;;
  *) echo "Usage: bash scripts/reproduce.sh [--offline]" >&2; exit 2 ;;
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
if [[ "$offline" == false ]]; then
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
