"""
Intraday quote loader: latest price for the securities currently held.

Flow:
  1. OAuth token from ORDS (client credentials, same client as the daily loader).
  2. Held symbols from the database (/loader/quote_symbols).
  3. Today's 1-minute bars from Yahoo via yfinance; the last bar is the current price.
  4. Send to the database (/loader/quotes). The table keeps only the latest quote per security.

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
            quotes.append({"yahoo_symbol": sym, "price": round(price, 6), "time": iso_with_colon(last.index[-1])})

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
