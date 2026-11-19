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


def floor_time(value, step):
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return EPOCH + ((value - EPOCH) // step) * step


def this_run_time(time_interval, ahead_seconds=5):
    return floor_time(utc_now(), interval_delta(time_interval))


def next_run_time(time_interval, ahead_seconds=5):
    step = interval_delta(time_interval)
    now = utc_now()
    target = floor_time(now, step) + step
    while target - now < timedelta(seconds=ahead_seconds):
        target += step
    return target


def sleep_until_run_time(time_interval, ahead_time=1, if_sleep=True):
    target = next_run_time(time_interval, ahead_time)
    if if_sleep:
        time.sleep(max(0, (target - utc_now()).total_seconds()))
    return target
