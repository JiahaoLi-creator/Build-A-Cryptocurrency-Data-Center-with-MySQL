"""Resume collection from a durable, per-symbol position in the closed UTC minute series."""

import sys
from collections import deque

import requests
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from Function import MINUTE, floor_time, utc_now
from windows import repair_window

CHUNK = 1000 * MINUTE


def checkpoint(engine, symbol, cutoff):
    """Initialize once from existing history or from the minute tracking starts."""
    select = "SELECT next_open_time FROM collector_checkpoint WHERE symbol = :symbol"
    with engine.begin() as conn:
        position = conn.scalar(text(select), {"symbol": symbol})
        if position is not None:
            return position
        oldest = conn.scalar(text(
            "SELECT MIN(candle_begin_time) FROM k WHERE symbol = :symbol "
            "AND time_interval = '1m' AND candle_begin_time < :cutoff"
        ), {"symbol": symbol, "cutoff": cutoff})
        conn.execute(text(
            "INSERT INTO collector_checkpoint (symbol, next_open_time) VALUES (:symbol, :position) "
            "ON DUPLICATE KEY UPDATE next_open_time = next_open_time"
        ), {"symbol": symbol, "position": oldest or cutoff})
        # A concurrent startup may have inserted the row after our first consistent read.
        return conn.scalar(text(select + " FOR UPDATE"), {"symbol": symbol})


def advance_checkpoint(engine, symbol, start, end):
    """Advance only when the verified interval covers the current position."""
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE collector_checkpoint SET next_open_time = :end WHERE symbol = :symbol "
            "AND next_open_time >= :start AND next_open_time < :end"
        ), {"symbol": symbol, "start": start, "end": end})


def catch_up(engine, selected, cutoff, *, fetch=None, stopping=None):
    """Verify a fixed closed-minute range in bounded, round-robin chunks."""
    pending = deque((symbol, checkpoint(engine, symbol, cutoff)) for symbol in selected)
    complete = True
    while pending:
        if stopping is not None and stopping.is_set():
            return False
        symbol, start = pending.popleft()
        if start >= cutoff:
            continue
        end = min(start + CHUNK, cutoff)
        try:
            missing = repair_window(engine, "k", symbol, start, end, fetch=fetch, now=cutoff)
        except (requests.RequestException, ValueError) as exc:
            print(f"{symbol} recovery will retry from {start} UTC: {exc}", file=sys.stderr, flush=True)
            complete = False
            continue
        position = end if missing.empty else missing[0].to_pydatetime()
        if position > start:
            advance_checkpoint(engine, symbol, start, position)
        print(f"{symbol} recovery [{start}, {end}) UTC | missing={len(missing)}", flush=True)
        if not missing.empty:
            complete = False
            print(f"{symbol} first missing in this range: {position} UTC", file=sys.stderr, flush=True)
        if end < cutoff:
            pending.append((symbol, end))
    return complete


def maintain_history(engine, selected, wake, stopping, once=False):
    """Run on startup, on reconnection and periodically while the service is alive."""
    while True:
        wake.clear()
        if not once and stopping.is_set():
            return False
        cutoff = floor_time(utc_now(), MINUTE)
        try:
            complete = catch_up(engine, selected, cutoff, stopping=None if once else stopping)
        except SQLAlchemyError as exc:
            print(f"Database recovery failed: {exc}", file=sys.stderr, flush=True)
            complete = False
        if once or stopping.is_set():
            return complete
        wake.wait(timeout=60)
