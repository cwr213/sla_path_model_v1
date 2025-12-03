"""Time utilities: timezone conversion, window alignment, dwell calculation."""
from datetime import datetime, time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from .config import SortWindow, MINUTES_PER_DAY

UTC = ZoneInfo("UTC")


def local_to_utc(local_dt: datetime, tz: ZoneInfo) -> datetime:
    if local_dt.tzinfo is not None:
        return local_dt.astimezone(UTC).replace(tzinfo=None)
    local_aware = local_dt.replace(tzinfo=tz)
    return local_aware.astimezone(UTC).replace(tzinfo=None)


def utc_to_local(utc_dt: datetime, tz: ZoneInfo) -> datetime:
    utc_aware = utc_dt.replace(tzinfo=UTC)
    return utc_aware.astimezone(tz).replace(tzinfo=None)


def time_to_minutes(t: time) -> int:
    return t.hour * 60 + t.minute


def minutes_to_time(mins: int) -> time:
    mins = mins % MINUTES_PER_DAY
    return time(mins // 60, mins % 60)


def is_time_in_window(check_time: time, window: SortWindow) -> bool:
    check_mins = time_to_minutes(check_time)
    start_mins = time_to_minutes(window.start_local)
    end_mins = time_to_minutes(window.end_local)

    if window.crosses_midnight():
        return check_mins >= start_mins or check_mins < end_mins
    else:
        return start_mins <= check_mins < end_mins


def align_to_window_end(
        target_utc: datetime,
        window: SortWindow,
        processing_minutes: float
) -> tuple[datetime, float]:
    """
    Backward-chain alignment: find when to start processing to finish by target,
    accounting for window constraints.

    Returns (processing_start_utc, dwell_minutes).
    """
    target_local = utc_to_local(target_utc, window.timezone)
    target_date = target_local.date()
    target_time = target_local.time()

    target_mins = time_to_minutes(target_time)
    start_mins = target_mins - processing_minutes

    days_back = 0
    while start_mins < 0:
        start_mins += MINUTES_PER_DAY
        days_back += 1

    proposed_start_local = datetime.combine(
        target_date - timedelta(days=days_back),
        minutes_to_time(int(start_mins))
    )

    proposed_start_time = proposed_start_local.time()

    if is_time_in_window(proposed_start_time, window):
        return local_to_utc(proposed_start_local, window.timezone), 0.0

    window_end_mins = time_to_minutes(window.end_local)
    window_start_mins = time_to_minutes(window.start_local)

    if window.crosses_midnight():
        window_duration = (MINUTES_PER_DAY - window_start_mins) + window_end_mins
    else:
        window_duration = window_end_mins - window_start_mins

    if processing_minutes > window_duration:
        processing_minutes = window_duration

    proposed_start_mins = time_to_minutes(proposed_start_time)

    if window.crosses_midnight():
        if proposed_start_mins < window_end_mins:
            window_end_date = target_date - timedelta(days=days_back) - timedelta(days=1)
        elif proposed_start_mins >= window_start_mins:
            window_end_date = target_date - timedelta(days=days_back)
        else:
            window_end_date = target_date - timedelta(days=days_back)
    else:
        if proposed_start_mins >= window_end_mins:
            window_end_date = target_date - timedelta(days=days_back)
        else:
            window_end_date = target_date - timedelta(days=days_back) - timedelta(days=1)

    actual_end_local = datetime.combine(window_end_date, window.end_local)
    actual_start_local = actual_end_local - timedelta(minutes=processing_minutes)

    actual_end_utc = local_to_utc(actual_end_local, window.timezone)
    dwell_minutes = (target_utc - actual_end_utc).total_seconds() / 60

    if dwell_minutes < 0:
        actual_end_local = actual_end_local - timedelta(days=1)
        actual_start_local = actual_end_local - timedelta(minutes=processing_minutes)
        actual_end_utc = local_to_utc(actual_end_local, window.timezone)
        dwell_minutes = (target_utc - actual_end_utc).total_seconds() / 60

    actual_start_utc = local_to_utc(actual_start_local, window.timezone)

    return actual_start_utc, max(0.0, dwell_minutes)


def align_to_window_start(
        ready_utc: datetime,
        window: SortWindow,
        processing_minutes: float
) -> tuple[datetime, float]:
    """
    Forward-chain alignment: find when processing can start given readiness time,
    accounting for window constraints.

    Returns (processing_start_utc, dwell_minutes).
    dwell_minutes = time spent waiting for window to open.
    """
    ready_local = utc_to_local(ready_utc, window.timezone)
    ready_date = ready_local.date()
    ready_time = ready_local.time()
    ready_mins = time_to_minutes(ready_time)

    window_start_mins = time_to_minutes(window.start_local)
    window_end_mins = time_to_minutes(window.end_local)

    # Check if ready time is within window
    if is_time_in_window(ready_time, window):
        # Can start immediately
        return ready_utc, 0.0

    # Need to wait for window to open
    # Find next window start
    if window.crosses_midnight():
        # Window like 22:00 - 06:00
        if ready_mins < window_start_mins and ready_mins >= window_end_mins:
            # We're in the gap (e.g., 10:00 when window is 22:00-06:00)
            # Next window start is today at window_start_mins
            next_start_local = datetime.combine(ready_date, window.start_local)
        else:
            # Shouldn't reach here if is_time_in_window is correct
            next_start_local = datetime.combine(ready_date, window.start_local)
    else:
        # Window like 06:00 - 22:00
        if ready_mins < window_start_mins:
            # Before window opens today
            next_start_local = datetime.combine(ready_date, window.start_local)
        else:
            # After window closed, next opening is tomorrow
            next_start_local = datetime.combine(
                ready_date + timedelta(days=1),
                window.start_local
            )

    next_start_utc = local_to_utc(next_start_local, window.timezone)
    dwell_minutes = (next_start_utc - ready_utc).total_seconds() / 60

    return next_start_utc, max(0.0, dwell_minutes)