"""Exchange-confirmed Spot suspensions, expressed as half-open UTC minute intervals."""

import csv
from dataclasses import dataclass
from datetime import datetime
from functools import cache
from pathlib import Path

import numpy as np
import pandas as pd

from Function import MINUTE, floor_time, parse_utc

CATALOG = Path(__file__).resolve().parent / "data" / "spot_suspensions.csv"


@dataclass(frozen=True)
class Suspension:
    start: datetime
    resume: datetime | None
    reason: str
    source_url: str


class SuspendedCandleError(ValueError):
    """A source supplied a candle inside a confirmed suspension."""


def utc_minute(value):
    stamp = parse_utc(value)
    if not value.endswith("Z") or floor_time(stamp, MINUTE) != stamp:
        raise ValueError(f"Expected a UTC minute ending in Z, got {value!r}")
    return stamp


@cache
def load_calendar(path=CATALOG):
    calendar = {}
    with path.open(encoding="utf-8", newline="") as stream:
        for row in csv.DictReader(stream):
            start = utc_minute(row["start_utc"])
            resume = utc_minute(row["resume_utc"]) if row["resume_utc"] else None
            symbol, reason, url = row["symbol"], row["reason"].strip(), row["source_url"]
            if (not symbol or symbol != symbol.upper() or not reason or not url.startswith("https://")
                    or (resume is not None and resume <= start)):
                raise ValueError(f"Invalid suspension for {symbol!r} at {start}")
            calendar.setdefault(symbol, []).append(Suspension(start, resume, reason, url))
    for symbol, events in calendar.items():
        events.sort(key=lambda event: event.start)
        for previous, current in zip(events, events[1:]):
            if previous.resume is None or previous.resume > current.start:
                raise ValueError(f"Overlapping suspensions for {symbol} at {current.start}")
    return calendar


def periods(symbol, start, end):
    """Partition [start, end) into expected trading ranges and documented suspensions."""
    if start >= end or floor_time(start, MINUTE) != start or floor_time(end, MINUTE) != end:
        raise ValueError("Expected a nonempty range aligned to UTC minutes")
    cursor = start
    for event in load_calendar().get(symbol.upper(), ()):
        if event.start >= end:
            break
        stop = min(event.resume or end, end)
        if stop <= cursor:
            continue
        if cursor < event.start:
            yield cursor, event.start, None
        yield max(cursor, event.start), stop, event
        cursor = stop
    if cursor < end:
        yield cursor, end, None


def suspension_at(symbol, minute):
    return next(periods(symbol, minute, minute + MINUTE))[2]


def suspended_mask(symbol, opens):
    blocked = np.zeros(len(opens), dtype=bool)
    events = load_calendar().get(symbol.upper(), ())
    if events:
        times = pd.DatetimeIndex(pd.to_datetime(opens, utc=True)).tz_convert(None)
        for event in events:
            inside = times >= event.start
            if event.resume is not None:
                inside &= times < event.resume
            blocked |= inside
    return blocked


def check_trading_rows(frame):
    """Refuse a conflicting source row before any part of the batch is written."""
    for symbol in frame["symbol"].unique():
        if symbol.upper() in load_calendar():
            times = frame.loc[(frame["symbol"] == symbol) & (frame["time_interval"] == "1m"), "candle_begin_time"]
            blocked = times[suspended_mask(symbol, times)]
            if not blocked.empty:
                raise SuspendedCandleError(f"{symbol} {blocked.iloc[0]} UTC conflicts with a documented trading suspension")


def eligible_bar_count(symbol, cutoff, step, limit):
    """Count complete UTC-aligned windows that lie entirely within trading periods."""
    end = floor_time(cutoff, step)
    count = 0
    for left, right, halt in periods(symbol, end - limit * step, end):
        if halt is None:
            first = floor_time(left, step)
            if first < left:
                first += step
            count += max(0, (right - first) // step)
    return count
