"""NYSE trading days, from 5.5 hours before open until session close (optionally a few minutes after it)."""
import datetime as dt
import zoneinfo
import pandas_market_calendars as mcal


def _session(now):
    day = now.astimezone(zoneinfo.ZoneInfo("America/New_York")).date()
    schedule = mcal.get_calendar("NYSE").schedule(start_date=day, end_date=day)
    if schedule.empty:
        return None
    return schedule.iloc[0]


def session_close(now=None):
    """Close of today's NYSE session (aware datetime), or None on a non-trading day."""
    now = now or dt.datetime.now(dt.timezone.utc)
    row = _session(now)
    return None if row is None else row.market_close.to_pydatetime()


def quote_window(now=None, after_close_min=0):
    """True from 5.5 hours before the open until the close plus after_close_min minutes.
    12/11/2026 (user): the intraday quote loader runs once more after the close, so the live
    change in the evening equals the nightly snapshot (the last runs before 16:00 New York missed
    the end of the session)."""
    now = now or dt.datetime.now(dt.timezone.utc)
    row = _session(now)
    if row is None:
        return False
    return (row.market_open.to_pydatetime() - dt.timedelta(hours=5.5) <= now
            < row.market_close.to_pydatetime() + dt.timedelta(minutes=after_close_min))
