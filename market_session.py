"""NYSE trading days, from 5.5 hours before open until session close."""
import datetime as dt
import pandas_market_calendars as mcal

def quote_window(now=None):
    now = now or dt.datetime.now(dt.timezone.utc)
    day = now.astimezone(__import__("zoneinfo").ZoneInfo("America/New_York")).date()
    schedule = mcal.get_calendar("NYSE").schedule(start_date=day, end_date=day)
    if schedule.empty:
        return False
    row = schedule.iloc[0]
    return row.market_open.to_pydatetime() - dt.timedelta(hours=5.5) <= now < row.market_close.to_pydatetime()
