#!/usr/bin/env python
"""Date helpers for the Alvys ingestion pipeline.

Calculates the **previous full Sunday‑to‑Saturday week** so every run is
idempotent and deterministic. All timestamps are timezone‑aware (UTC by
default) and precise to the millisecond.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Tuple

__all__ = [
    "get_last_week_range",
    "iso_range",
]


def _start_of_week(ref: datetime, tz: timezone) -> datetime:
    """Return the *current* week’s Sunday 00:00 for ``ref`` (tz‑aware)."""
    ref = ref.astimezone(tz)
    days_since_sunday = (ref.weekday() + 1) % 7  # Monday=0 → Sunday=6 → want Sun=0
    sunday = (ref - timedelta(days=days_since_sunday)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
        tzinfo=tz,
    )
    return sunday


def get_last_week_range(
    reference: datetime | None = None,
    tz: timezone = timezone.utc,
) -> Tuple[datetime, datetime]:
    """Return *last* week’s (Sunday 00:00, Saturday 23:59:59.999) tuple.

    Example – if *today* is **Mon 2025‑06‑09 14:05 UTC**:

    >>> start, end = get_last_week_range()
    >>> start  # 2025‑06‑01 00:00:00+00:00
    >>> end    # 2025‑06‑07 23:59:59.999000+00:00
    """
    if reference is None:
        reference = datetime.now(tz)

    # Beginning of *current* week (this week’s Sunday 00:00)
    this_week_sunday = _start_of_week(reference, tz)

    # Beginning of *last* week
    last_week_sunday = this_week_sunday - timedelta(days=7)

    start_dt = last_week_sunday
    end_dt = this_week_sunday - timedelta(milliseconds=1)

    return start_dt, end_dt


def iso_range(
    reference: datetime | None = None,
    tz: timezone = timezone.utc,
) -> Tuple[str, str]:
    """Convenience wrapper that returns the tuple as ISO‑8601 strings exactly
    to the millisecond (\*YYYY‑MM‑DDTHH:MM:SS.mmmZ*)."""

    start_dt, end_dt = get_last_week_range(reference, tz)
    return (
        start_dt.isoformat(timespec="milliseconds"),
        end_dt.isoformat(timespec="milliseconds"),
    )
