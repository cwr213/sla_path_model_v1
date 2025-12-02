"""
Time utilities: timezone conversion, window alignment, dwell calculation.
"""
from datetime import datetime, time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from .config import SortWindow, MINUTES_PER_DAY

UTC = ZoneInfo("UTC")


def local_to_utc(local_dt: datetime, tz: ZoneInfo) -> datetime:
    """
    Convert local datetime to UTC.

    Args:
        local_dt: Naive datetime representing local time
        tz: Timezone of the local time

    Returns:
        Naive datetime in UTC
    """
    if local_dt.tzinfo is not None:
        # Already has timezone, convert directly
        return local_dt.astimezone(UTC).replace(tzinfo=None)

    # Attach timezone and convert
    local_aware = local_dt.replace(tzinfo=tz)
    return local_aware.astimezone(UTC).replace(tzinfo=None)


def utc_to_local(utc_dt: datetime, tz: ZoneInfo) -> datetime:
    """
    Convert UTC datetime to local time.

    Args:
        utc_dt: Naive datetime in UTC
        tz: Target timezone

    Returns:
        Naive datetime in local time
    """
    utc_aware = utc_dt.replace(tzinfo=UTC)
    return utc_aware.astimezone(tz).replace(tzinfo=None)


def time_to_minutes(t: time) -> int:
    """Convert time to minutes since midnight."""
    return t.hour * 60 + t.minute


def minutes_to_time(mins: int) -> time:
    """Convert minutes since midnight to time object."""
    mins = mins % MINUTES_PER_DAY
    return time(mins // 60, mins % 60)


def is_time_in_window(check_time: time, window: SortWindow) -> bool:
    """
    Check if a time falls within a sort window.

    Handles windows that cross midnight (e.g., 22:00 to 06:00).
    """
    check_mins = time_to_minutes(check_time)
    start_mins = time_to_minutes(window.start_local)
    end_mins = time_to_minutes(window.end_local)

    if window.crosses_midnight():
        # Window like 22:00-06:00: valid if time >= 22:00 OR time < 06:00
        return check_mins >= start_mins or check_mins < end_mins
    else:
        # Normal window like 06:00-14:00
        return start_mins <= check_mins < end_mins


def next_window_start_utc(current_utc: datetime, window: SortWindow) -> datetime:
    """
    Find the next occurrence of window start time at or after current_utc.

    Used for forward-chaining when waiting for a sort window to open.
    """
    # Convert current UTC to local
    current_local = utc_to_local(current_utc, window.timezone)

    # Get current date and time in local
    current_date = current_local.date()
    current_time = current_local.time()

    # Create candidate window start on current date
    candidate = datetime.combine(current_date, window.start_local)

    # If current time is already past window start, check if we're still in window
    if window.crosses_midnight():
        # For midnight-crossing windows, start is in evening
        # If we're in the morning portion (before end), we're still in yesterday's window
        end_mins = time_to_minutes(window.end_local)
        current_mins = time_to_minutes(current_time)
        if current_mins < end_mins:
            # We're in the window that started yesterday
            candidate = datetime.combine(current_date - timedelta(days=1), window.start_local)
        elif current_mins >= time_to_minutes(window.start_local):
            # We're in today's window or at the start
            pass
        else:
            # We're between end and start (middle of day for overnight window)
            # Next start is today
            pass
    else:
        # Normal window
        if current_time >= window.start_local:
            if current_time < window.end_local:
                # Already in window, return current time
                return current_utc
            else:
                # Past window, next start is tomorrow
                candidate = datetime.combine(current_date + timedelta(days=1), window.start_local)

    # Convert back to UTC
    return local_to_utc(candidate, window.timezone)


def previous_window_end_utc(current_utc: datetime, window: SortWindow) -> datetime:
    """
    Find the most recent window end time at or before current_utc.

    Used for backward-chaining to align with delivery deadlines.
    """
    # Convert current UTC to local
    current_local = utc_to_local(current_utc, window.timezone)

    current_date = current_local.date()
    current_time = current_local.time()

    # Create candidate window end on current date
    candidate = datetime.combine(current_date, window.end_local)

    if window.crosses_midnight():
        # End time is in the morning (e.g., 06:00 for 22:00-06:00 window)
        end_mins = time_to_minutes(window.end_local)
        current_mins = time_to_minutes(current_time)

        if current_mins < end_mins:
            # Current is before end today, so we're in today's window
            # Previous end was today's end (coming up) - no, wait
            # If current is 04:00 and window ends at 06:00, the PREVIOUS end was yesterday at 06:00
            # But we want the upcoming end... this is for backward chaining
            # For backward chaining from delivery deadline, we want the most recent end <= current
            # If current is 04:00 and end is 06:00, previous end was yesterday at 06:00
            candidate = datetime.combine(current_date - timedelta(days=1), window.end_local)
        else:
            # Current is after end today, previous end was today
            pass
    else:
        # Normal window
        if current_time < window.end_local:
            # Before today's end, previous end was yesterday
            candidate = datetime.combine(current_date - timedelta(days=1), window.end_local)

    return local_to_utc(candidate, window.timezone)


def align_to_window_end(
        target_utc: datetime,
        window: SortWindow,
        processing_minutes: float
) -> tuple[datetime, float]:
    """
    Backward-chain alignment: given a target completion time, find when to start
    processing to finish by target, accounting for window constraints.

    Returns:
        (processing_start_utc, dwell_minutes): When processing starts, and any
        dwell time waiting for window to open.
    """
    # Convert target to local
    target_local = utc_to_local(target_utc, window.timezone)
    target_date = target_local.date()
    target_time = target_local.time()

    # Calculate when processing would need to start (in local time)
    target_mins = time_to_minutes(target_time)
    start_mins = target_mins - processing_minutes

    # Handle day rollback if needed
    days_back = 0
    while start_mins < 0:
        start_mins += MINUTES_PER_DAY
        days_back += 1

    proposed_start_local = datetime.combine(
        target_date - timedelta(days=days_back),
        minutes_to_time(int(start_mins))
    )

    # Check if proposed start falls within window
    proposed_start_time = proposed_start_local.time()

    if is_time_in_window(proposed_start_time, window):
        # We can start at proposed time, no dwell
        return local_to_utc(proposed_start_local, window.timezone), 0.0

    # Need to find earlier window that can accommodate our processing time
    # Work backward to find a window end where we can fit processing
    window_end_mins = time_to_minutes(window.end_local)
    window_start_mins = time_to_minutes(window.start_local)

    if window.crosses_midnight():
        window_duration = (MINUTES_PER_DAY - window_start_mins) + window_end_mins
    else:
        window_duration = window_end_mins - window_start_mins

    # If processing time exceeds window duration, we have a problem
    if processing_minutes > window_duration:
        # Can't fit - for now, just use window duration and note the overflow
        processing_minutes = window_duration

    # Find the window end that's before our proposed start
    # Then back up by processing time
    proposed_start_mins = time_to_minutes(proposed_start_time)

    # Calculate the most recent window end before proposed start
    if window.crosses_midnight():
        # Window ends in morning
        if proposed_start_mins < window_end_mins:
            # We're in morning before window end - use previous day's window end
            window_end_date = target_date - timedelta(days=days_back) - timedelta(days=1)
        elif proposed_start_mins >= window_start_mins:
            # We're in evening during window - use today's upcoming end (tomorrow morning)
            # But for backward chaining, we want the end that just passed
            window_end_date = target_date - timedelta(days=days_back)
        else:
            # We're in the gap (daytime)
            # Previous window end was this morning
            window_end_date = target_date - timedelta(days=days_back)
    else:
        # Normal window ends same day
        if proposed_start_mins >= window_end_mins:
            # After window end, use today's window end
            window_end_date = target_date - timedelta(days=days_back)
        else:
            # Before window end, use yesterday's window end
            window_end_date = target_date - timedelta(days=days_back) - timedelta(days=1)

    # Set processing to end at window end
    actual_end_local = datetime.combine(window_end_date, window.end_local)
    actual_start_local = actual_end_local - timedelta(minutes=processing_minutes)

    # Calculate dwell: time from when we would have started to when target occurred
    actual_end_utc = local_to_utc(actual_end_local, window.timezone)
    dwell_minutes = (target_utc - actual_end_utc).total_seconds() / 60

    # Dwell should be positive (we finished earlier than target)
    if dwell_minutes < 0:
        # This means we couldn't finish in time - need to go back another day
        # Recursively try previous day's window
        actual_end_local = actual_end_local - timedelta(days=1)
        actual_start_local = actual_end_local - timedelta(minutes=processing_minutes)
        actual_end_utc = local_to_utc(actual_end_local, window.timezone)
        dwell_minutes = (target_utc - actual_end_utc).total_seconds() / 60

    actual_start_utc = local_to_utc(actual_start_local, window.timezone)

    return actual_start_utc, max(0.0, dwell_minutes)


def find_latest_cpt_before(
        target_utc: datetime,
        cpts: list[tuple[time, ZoneInfo]],  # List of (cpt_local_time, timezone) tuples
        reference_date: datetime
) -> tuple[datetime, float]:
    """
    Find the latest CPT departure that arrives before target_utc.

    Args:
        target_utc: Must arrive by this time
        cpts: Available CPT times (local time + timezone)
        reference_date: Reference date for calculations

    Returns:
        (cpt_departure_utc, dwell_minutes): CPT departure time and dwell waiting for it
    """
    # Convert target to reference timezone (use first CPT's timezone)
    if not cpts:
        # No CPTs, return target with no dwell
        return target_utc, 0.0

    tz = cpts[0][1]
    target_local = utc_to_local(target_utc, tz)
    target_date = target_local.date()

    # Build list of CPT datetimes in UTC
    cpt_datetimes_utc = []
    for cpt_time, cpt_tz in cpts:
        # Try today and yesterday
        for day_offset in [0, -1]:
            cpt_local = datetime.combine(target_date + timedelta(days=day_offset), cpt_time)
            cpt_utc = local_to_utc(cpt_local, cpt_tz)
            if cpt_utc <= target_utc:
                cpt_datetimes_utc.append(cpt_utc)

    if not cpt_datetimes_utc:
        # No valid CPT found, go back further
        for cpt_time, cpt_tz in cpts:
            cpt_local = datetime.combine(target_date - timedelta(days=2), cpt_time)
            cpt_utc = local_to_utc(cpt_local, cpt_tz)
            cpt_datetimes_utc.append(cpt_utc)

    # Find latest CPT before target
    valid_cpts = [c for c in cpt_datetimes_utc if c <= target_utc]

    if valid_cpts:
        latest_cpt = max(valid_cpts)
        dwell_minutes = (target_utc - latest_cpt).total_seconds() / 60
        return latest_cpt, dwell_minutes

    # Fall back to earliest CPT if none before target
    earliest_cpt = min(cpt_datetimes_utc)
    return earliest_cpt, 0.0