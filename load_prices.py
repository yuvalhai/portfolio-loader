"""
Daily price and FX loader for the portfolio database.

Flow:
  1. Get an OAuth token from ORDS (client credentials).
  2. Read the list of Yahoo symbols from the database (/loader/symbols).
  3. Download daily prices from Yahoo via yfinance, in batches.
  4. Send prices to the database (/loader/prices), in chunks.
  5. Read Bank of Israel representative rates and send them (/loader/fx).
  6. For securities whose name did not come from Yahoo yet, read the name
     from Yahoo and send it (/loader/names). Names always come from Yahoo.

Environment variables (set as GitHub secrets):
  ORDS_BASE            e.g. https://<host>/ords/portfolio
  ORDS_CLIENT_ID
  ORDS_CLIENT_SECRET
  HISTORY_PERIOD       optional, default "5d". Use e.g. "3y" once for backfill.
                       Securities with little history (need_history = Y from the
                       database) always get FULL_HISTORY_PERIOD, so a new security
                       has ATR from its first night.
"""

import os
import sys
import math
import time
from datetime import datetime

import requests
import pandas as pd
import yfinance as yf

ORDS_BASE = os.environ["ORDS_BASE"].rstrip("/")
CLIENT_ID = os.environ["ORDS_CLIENT_ID"]
CLIENT_SECRET = os.environ["ORDS_CLIENT_SECRET"]
PERIOD = os.environ.get("HISTORY_PERIOD", "5d") or "5d"
FULL_HISTORY_PERIOD = "3y"

DOWNLOAD_BATCH = 50      # tickers per yfinance download call
POST_CHUNK_ROWS = 5000   # price rows per POST to the database
BOI_URL = "https://boi.org.il/PublicApi/GetExchangeRates"
FX_CURRENCIES = {"USD", "GBP", "EUR"}


def get_token():
    r = requests.post(
        f"{ORDS_BASE}/oauth/token",
        data={"grant_type": "client_credentials"},
        auth=(CLIENT_ID, CLIENT_SECRET),
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def api_get(token, path):
    r = requests.get(
        f"{ORDS_BASE}/loader/{path}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def api_post(token, path, payload):
    r = requests.post(
        f"{ORDS_BASE}/loader/{path}",
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
        timeout=120,
    )
    r.raise_for_status()
    result = r.json()
    if result.get("status") != "OK":
        raise RuntimeError(f"Database rejected {path}: {result}")
    return result


def num(x):
    """Convert to a JSON-safe float, or None for missing values."""
    if x is None:
        return None
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else round(f, 6)


def frame_for(data, symbol, batch_size):
    """Extract one ticker's frame from a yfinance download result."""
    if data is None or data.empty:
        return None
    if not isinstance(data.columns, pd.MultiIndex):
        return data if batch_size == 1 else None
    if symbol not in data.columns.get_level_values(0):
        return None
    return data[symbol]


def download_prices(symbols, period):
    rows, failed = [], []
    for i in range(0, len(symbols), DOWNLOAD_BATCH):
        batch = symbols[i:i + DOWNLOAD_BATCH]
        data = yf.download(
            batch,
            period=period,
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            threads=True,
            progress=False,
        )
        for sym in batch:
            df = frame_for(data, sym, len(batch))
            if df is None or df.empty or df["Close"].dropna().empty:
                failed.append(sym)
                continue
            df = df.dropna(subset=["Close"]).copy()
            df["Prev_Close"] = df["Close"].shift(1)
            for dt, rec in df.iterrows():
                rows.append({
                    "yahoo_symbol": sym,
                    "date": pd.Timestamp(dt).strftime("%Y-%m-%d"),
                    "open": num(rec.get("Open")),
                    "high": num(rec.get("High")),
                    "low": num(rec.get("Low")),
                    "close": num(rec.get("Close")),
                    "prev_close": num(rec.get("Prev_Close")),
                    "volume": num(rec.get("Volume")),
                })
        time.sleep(1)
    return rows, failed


def load_fx(token):
    r = requests.get(BOI_URL, timeout=30)
    r.raise_for_status()
    rates = []
    for item in r.json()["exchangeRates"]:
        if item["key"] not in FX_CURRENCIES:
            continue
        rate_date = datetime.fromisoformat(item["lastUpdate"].replace("Z", "")[:19]).strftime("%Y-%m-%d")
        rates.append({
            "currency": item["key"],
            "date": rate_date,
            "rate_to_ils": item["currentExchangeRate"] / item["unit"],
        })
    return api_post(token, "fx", {"source": "BOI", "rates": rates})


def fetch_names(symbols):
    """Read the company name from Yahoo for each symbol. Failures are skipped."""
    names = []
    for sym in symbols:
        try:
            info = yf.Ticker(sym).get_info() or {}
            name = info.get("longName") or info.get("shortName")
            if name:
                names.append({"yahoo_symbol": sym, "name": str(name).strip()})
        except Exception as e:  # rate limit, unknown symbol, network
            print(f"Name lookup failed for {sym}: {e}")
        time.sleep(0.3)
    return names


def main():
    token = get_token()

    sym_rows = api_get(token, "symbols")
    symbols = [s["yahoo_symbol"] for s in sym_rows]
    need_name = [s["yahoo_symbol"] for s in sym_rows if s.get("need_name") == "Y"]
    print(f"Symbols from database: {len(symbols)}; period: {PERIOD}")

    need_history = [s["yahoo_symbol"] for s in sym_rows if s.get("need_history") == "Y"]
    if PERIOD == FULL_HISTORY_PERIOD:
        need_history = []
    regular = [s for s in symbols if s not in set(need_history)]

    rows, failed = download_prices(regular, PERIOD)
    if need_history:
        print(f"Full history ({FULL_HISTORY_PERIOD}) for {len(need_history)} symbols: {need_history}")
        h_rows, h_failed = download_prices(need_history, FULL_HISTORY_PERIOD)
        rows += h_rows
        failed += h_failed
    print(f"Price rows: {len(rows)}; failed symbols: {failed}")

    if not rows:
        api_post(token, "prices", {"source": "yfinance", "prices": [], "failed": failed})
        print("No prices downloaded.")
        sys.exit(1)

    for i in range(0, len(rows), POST_CHUNK_ROWS):
        chunk = rows[i:i + POST_CHUNK_ROWS]
        is_last = i + POST_CHUNK_ROWS >= len(rows)
        res = api_post(token, "prices", {
            "source": "yfinance",
            "prices": chunk,
            "failed": failed if is_last else [],
        })
        print(f"Posted {len(chunk)} rows: {res}")

    print("FX:", load_fx(token))

    if need_name:
        names = fetch_names(need_name)
        print(f"Names found: {len(names)} of {len(need_name)}")
        if names:
            token = get_token()  # the price step may have taken long
            print("Names:", api_post(token, "names", {"names": names}))


if __name__ == "__main__":
    main()
