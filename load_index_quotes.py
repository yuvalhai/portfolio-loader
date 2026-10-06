"""Refresh only INDEX quotes; no research or full portfolio refresh."""
import datetime as dt
import math
from zoneinfo import ZoneInfo
import requests
import yfinance as yf
from load_quotes import ORDS_BASE, get_token, frame_for, iso_with_colon

def collect(rows):
    quotes = []
    tv_rows = [r for r in rows if r.get("tv") and r["yahoo_symbol"] not in ("BTC-USD", "ETH-USD")]
    yahoo_rows = [r for r in rows if r not in tv_rows]
    if yahoo_rows:
        symbols = [r["yahoo_symbol"] for r in yahoo_rows]
        data = yf.download(symbols, period="5d", interval="1m", group_by="ticker",
                           auto_adjust=False, prepost=True, progress=False)
        daily = yf.download(symbols, period="5d", interval="1d", group_by="ticker",
                            auto_adjust=False, progress=False)
        for sym in symbols:
            df, hist = frame_for(data, sym, len(symbols)), frame_for(daily, sym, len(symbols))
            if df is None or hist is None:
                continue
            last = df["Close"].dropna()
            if last.empty:
                continue
            price, stamp = float(last.iloc[-1]), last.index[-1]
            prior = hist["Close"].dropna()
            today = dt.datetime.now(ZoneInfo("UTC" if sym in ("BTC-USD", "ETH-USD") else "America/New_York")).date()
            prior = prior[[x.date() < today for x in prior.index]]
            if prior.empty or not math.isfinite(price):
                continue
            pct = 100 * (price / float(prior.iloc[-1]) - 1)
            quotes.append({"yahoo_symbol": sym, "price": price, "change_pct": pct,
                           "time": iso_with_colon(stamp), "source": "yfinance"})
    if tv_rows:
        response = requests.post("https://scanner.tradingview.com/global/scan",
            json={"symbols": {"tickers": [r["tv"] for r in tv_rows], "query": {"types": []}},
                  "columns": ["close", "change"]}, timeout=30)
        response.raise_for_status()
        by_tv = {r["tv"]: r["yahoo_symbol"] for r in tv_rows}
        observed = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        for item in response.json().get("data", []):
            price, pct = item["d"]
            if price is None or pct is None:
                continue
            quotes.append({"yahoo_symbol": by_tv[item["s"]], "price": price,
                           "change_pct": pct, "time": observed, "source": "TV_observed"})
    return quotes

def main():
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"{ORDS_BASE}/loader/index_symbols", headers=headers, timeout=60)
    r.raise_for_status()
    rows = r.json()
    quotes = collect(rows)
    received = {q["yahoo_symbol"] for q in quotes}
    missing = [r["yahoo_symbol"] for r in rows if r["yahoo_symbol"] not in received]
    print(f"Index quotes: {len(quotes)}/{len(rows)}; missing: {missing}")
    if not quotes:
        raise RuntimeError("No index quotes received")
    r = requests.post(f"{ORDS_BASE}/loader/index_quotes", headers=headers,
                      json={"quotes": quotes}, timeout=60)
    r.raise_for_status()
    result = r.json()
    print(result)
    if result.get("status") != "OK" or result.get("rows", 0) != len(quotes):
        raise RuntimeError("Index quote write failed")

if __name__ == "__main__":
    main()
