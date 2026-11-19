"""UTC boundaries used by the collector, repair job and reader."""

import re
import time
from datetime import datetime, timedelta, timezone

EPOCH = datetime(1970, 1, 1)
MINUTE = timedelta(minutes=1)


def utc_now():
    """Naive UTC, matching the MySQL DATETIME storage convention."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def parse_utc(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def interval_delta(value):
    match = re.fullmatch(r"([1-9][0-9]*)([mhTH])", value)
    if not match:
        raise ValueError(f"Expected a positive minute or hour interval, got {value!r}")
    size, unit = match.groups()
    return timedelta(minutes=int(size) * (60 if unit in ("h", "H") else 1))


def minute_offset(step, minutes):
    if type(minutes) is not int or not 0 <= minutes < step / MINUTE:
        raise ValueError("--offset-minutes must be a whole number from zero up to (but excluding) the interval")
    return minutes * MINUTE


def floor_time(value, step, *, offset_minutes=0):
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    origin = EPOCH + minute_offset(step, offset_minutes)
    return origin + ((value - origin) // step) * step


def this_run_time(time_interval, ahead_seconds=5, *, offset_minutes=0):
    return floor_time(utc_now(), interval_delta(time_interval), offset_minutes=offset_minutes)


def next_run_time(time_interval, ahead_seconds=5, *, offset_minutes=0):
    step = interval_delta(time_interval)
    now = utc_now()
    target = floor_time(now, step, offset_minutes=offset_minutes) + step
    while target - now < timedelta(seconds=ahead_seconds):
        target += step
    return target


def sleep_until_run_time(time_interval, ahead_time=1, if_sleep=True, *, offset_minutes=0):
    target = next_run_time(time_interval, ahead_time, offset_minutes=offset_minutes)
    if if_sleep:
        time.sleep(max(0, (target - utc_now()).total_seconds()))
    return target
