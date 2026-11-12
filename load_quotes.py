"""
Intraday quote loader: latest price for the securities currently held.

Flow:
  1. OAuth token from ORDS (client credentials, same client as the daily loader).
  2. Held symbols from the database (/loader/quote_symbols).
  3. Today's 1-minute bars from Yahoo via yfinance; the last bar is the current price.
     From the same bars: the day's open, high, low and cumulative volume so far (current
     trading day in America/New_York, regular session only).
  4. Send to the database (/loader/quotes). The table keeps only the latest quote per security.
     The day fields are optional for the database: a quote without them is stored as before.

Post-close run (12/11/2026, user): the window stays open AFTER_CLOSE_MIN minutes after the NYSE
close. In that run the price is the regular-session close (Yahoo daily bar of the session date,
else the last 1-minute bar before the close), never an after-hours bar, and the quote time is the
close. So the live change in the evening equals the nightly snapshot.

Environment variables (GitHub secrets): ORDS_BASE, ORDS_CLIENT_ID, ORDS_CLIENT_SECRET
"""

import os
import sys
import math
import datetime as dt
from market_session import quote_window, session_close

import requests
import pandas as pd
import yfinance as yf

ORDS_BASE = os.environ["ORDS_BASE"].rstrip("/")
CLIENT_ID = os.environ["ORDS_CLIENT_ID"]
CLIENT_SECRET = os.environ["ORDS_CLIENT_SECRET"]
BATCH = 50
SESSION_TZ = "America/New_York"
AFTER_CLOSE_MIN = 20


def get_token():
    r = requests.post(f"{ORDS_BASE}/oauth/token", data={"grant_type": "client_credentials"},
                      auth=(CLIENT_ID, CLIENT_SECRET), timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def iso_with_colon(ts):
    """2026-09-21T15:45:00-04:00 (the database expects a colon in the offset)."""
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    z = ts.strftime("%z")
    return ts.strftime("%Y-%m-%dT%H:%M:%S") + z[:3] + ":" + z[3:]


def frame_for(data, symbol, batch_size):
    if data is None or data.empty:
        return None
    if not isinstance(data.columns, pd.MultiIndex):
        return data if batch_size == 1 else None
    if symbol not in data.columns.get_level_values(0):
        return None
    return data[symbol]


def num(x, digits=6):
    """JSON-safe float, or None for missing values."""
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(f):
        return None
    return int(round(f)) if digits == 0 else round(f, digits)


def day_ohlc(df):
    """Open, high, low and cumulative volume of the current trading day so far, from 1-minute bars.

    The day is the America/New_York date of the last bar with a close; only bars of that date count
    (the download is regular session only). Returns a dict with the keys that have a value."""
    bars = df.dropna(subset=["Close"])
    if bars.empty:
        return {}
    idx = pd.DatetimeIndex(bars.index)
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    ny_dates = idx.tz_convert(SESSION_TZ).date
    bars = bars[ny_dates == ny_dates[-1]]
    local = pd.DatetimeIndex(bars.index).tz_convert(SESSION_TZ)
    bars = bars[(local.hour * 60 + local.minute >= 570) & (local.hour * 60 + local.minute < 960)]
    if bars.empty:
        return {}
    out = {
        "day_open": num(bars["Open"].dropna().iloc[0]) if not bars["Open"].dropna().empty else None,
        "day_high": num(bars["High"].max()),
        "day_low": num(bars["Low"].min()),
        "day_volume": num(bars["Volume"].sum(min_count=1), 0) if "Volume" in bars else None,
    }
    return {k: v for k, v in out.items() if v is not None}


def before_close(series, close_ts):
    """The part of a bar series strictly before the session close (regular session, no after-hours)."""
    idx = pd.DatetimeIndex(series.index)
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    return series[idx < pd.Timestamp(close_ts)]


def daily_close(daily, sym, batch_size, close_ts):
    """Close of the session date from the Yahoo daily bar, or None."""
    df = frame_for(daily, sym, batch_size)
    if df is None or df.empty or "Close" not in df:
        return None
    c = df["Close"].dropna()
    if c.empty:
        return None
    session_day = pd.Timestamp(close_ts).tz_convert(SESSION_TZ).date()
    last_day = pd.Timestamp(c.index[-1]).date()
    if last_day != session_day:
        return None
    v = num(c.iloc[-1])
    return v if v and v > 0 else None


def main():
    if not quote_window(after_close_min=AFTER_CLOSE_MIN):
        print("Outside NYSE quote window; skipped")
        return
    close_ts = session_close()
    after_close = close_ts is not None and dt.datetime.now(dt.timezone.utc) >= close_ts
    if after_close:
        print(f"Post-close run: regular-session close of {close_ts.isoformat()}")

    token = get_token()
    r = requests.get(f"{ORDS_BASE}/loader/quote_symbols",
                     headers={"Authorization": f"Bearer {token}"}, timeout=60)
    r.raise_for_status()
    symbols = [s["yahoo_symbol"] for s in r.json()]
    print(f"Held symbols: {len(symbols)}")

    quotes, failed = [], []
    for i in range(0, len(symbols), BATCH):
        batch = symbols[i:i + BATCH]
        data = yf.download(batch, period="1d", interval="1m", group_by="ticker",
                           auto_adjust=False, prepost=True, threads=True, progress=False)
        daily = None
        if after_close:
            try:
                daily = yf.download(batch, period="5d", interval="1d", group_by="ticker",
                                    auto_adjust=False, threads=True, progress=False)
            except Exception as e:  # fall back to the last 1-minute bar before the close
                print(f"Daily bars failed: {e}")
        for sym in batch:
            df = frame_for(data, sym, len(batch))
            if df is None or df.empty or df["Close"].dropna().empty:
                failed.append(sym)
                continue
            last = df["Close"].dropna()
            if after_close:
                price = daily_close(daily, sym, len(batch), close_ts)
                if price is None:
                    reg = before_close(last, close_ts)
                    if reg.empty:
                        failed.append(sym)
                        continue
                    price = float(reg.iloc[-1])
                qtime = close_ts
            else:
                price = float(last.iloc[-1])
                qtime = last.index[-1]
            if price is None or math.isnan(price):
                failed.append(sym)
                continue
            quote = {"yahoo_symbol": sym, "price": round(price, 6), "time": iso_with_colon(qtime)}
            try:
                quote.update(day_ohlc(df))
            except Exception as e:  # the day fields are optional; never lose the price because of them
                print(f"Day OHLC failed for {sym}: {e}")
            quotes.append(quote)

    print(f"Quotes: {len(quotes)}; failed: {failed}")
    if not quotes:
        sys.exit(1)

    r = requests.post(f"{ORDS_BASE}/loader/quotes", json={"source": "yfinance", "quotes": quotes},
                      headers={"Authorization": f"Bearer {token}"}, timeout=120)
    r.raise_for_status()
    res = r.json()
    print("Result:", res)
    if res.get("status") != "OK":
        sys.exit(1)


if __name__ == "__main__":
    main()
