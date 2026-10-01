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

Environment variables (GitHub secrets): ORDS_BASE, ORDS_CLIENT_ID, ORDS_CLIENT_SECRET
"""

import os
import sys
import math

import requests
import pandas as pd
import yfinance as yf

ORDS_BASE = os.environ["ORDS_BASE"].rstrip("/")
CLIENT_ID = os.environ["ORDS_CLIENT_ID"]
CLIENT_SECRET = os.environ["ORDS_CLIENT_SECRET"]
BATCH = 50
SESSION_TZ = "America/New_York"


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
    out = {
        "day_open": num(bars["Open"].dropna().iloc[0]) if not bars["Open"].dropna().empty else None,
        "day_high": num(bars["High"].max()),
        "day_low": num(bars["Low"].min()),
        "day_volume": num(bars["Volume"].sum(min_count=1), 0) if "Volume" in bars else None,
    }
    return {k: v for k, v in out.items() if v is not None}


def main():
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
                           auto_adjust=False, prepost=False, threads=True, progress=False)
        for sym in batch:
            df = frame_for(data, sym, len(batch))
            if df is None or df.empty or df["Close"].dropna().empty:
                failed.append(sym)
                continue
            last = df["Close"].dropna()
            price = float(last.iloc[-1])
            if math.isnan(price):
                failed.append(sym)
                continue
            quote = {"yahoo_symbol": sym, "price": round(price, 6), "time": iso_with_colon(last.index[-1])}
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
