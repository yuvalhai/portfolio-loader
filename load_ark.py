"""
Daily: ARK funds combined (cathiesark.com) -> recommendations of 'ARK Invest'.
Reads the complete-holdings table (ticker, weight) and the trades table (date, fund, ticker, direction,
% of position) and posts both to ORDS /loader/ark. The rules run in the DB (PF_ARK_PKG).
Secrets: ORDS_BASE, ORDS_CLIENT_ID, ORDS_CLIENT_SECRET
"""
import json
import os
import re
import sys
from datetime import datetime

import httpx

from load_granny import HEADERS, Tables

ORDS_BASE = os.environ["ORDS_BASE"].rstrip("/")
HOLDINGS_URL = "https://cathiesark.com/ark-funds-combined/complete-holdings"
TRADES_URL = "https://cathiesark.com/ark-funds-combined/trades"
TICKER_RE = re.compile(r"^[A-Z][A-Z.]{0,6}$")


def ticker(cell):
    """The ticker cell may also hold a logo text ('CRWV logo CRWV' or 'SL SLMT'): take the last ticker-like token."""
    toks = [t for t in cell.replace("\xa0", " ").split() if TICKER_RE.match(t) and t != "logo"]
    return toks[-1] if toks else None


def pct(s):
    try:
        return float(s.replace("%", "").replace(",", "").strip())
    except (ValueError, AttributeError):
        return None


def table(html, must):
    p = Tables()
    p.feed(html)
    for t in p.tables:
        if t and all(any(m.lower() == h.lower() for h in t[0]) for m in must):
            return [h.lower() for h in t[0]], t[1:]
    return None, []


def holdings(html):
    head, rows = table(html, ["Ticker", "Weight"])
    if not head:
        return []
    it, iw = head.index("ticker"), head.index("weight")
    out = []
    for r in rows:
        if len(r) > max(it, iw):
            t, w = ticker(r[it]), pct(r[iw])
            if t and w is not None:
                out.append({"t": t, "w": w})
    return out


def trades(html):
    head, rows = table(html, ["Date", "Ticker", "Direction", "% of Position"])
    if not head:
        return []
    idx = {k: head.index(k) for k in ("date", "fund", "ticker", "direction", "% of position") if k in head}
    out = []
    for r in rows:
        if len(r) <= max(idx.values()):
            continue
        t, pp = ticker(r[idx["ticker"]]), pct(r[idx["% of position"]])
        d = r[idx["direction"]].strip().capitalize()
        if not t or pp is None or d not in ("Buy", "Sell"):
            continue
        try:
            dt = datetime.strptime(r[idx["date"]].strip(), "%b %d, %Y").strftime("%Y-%m-%d")
        except ValueError:
            dt = None
        out.append({"d": dt, "f": r[idx["fund"]].strip() if "fund" in idx else None, "t": t, "dir": d, "pp": pp})
    return out


def main():
    with httpx.Client(headers=HEADERS, timeout=60, follow_redirects=True) as web:
        rh = web.get(HOLDINGS_URL)
        rh.raise_for_status()
        rt = web.get(TRADES_URL)
        rt.raise_for_status()
        h, tr = holdings(rh.text), trades(rt.text)
    dates = sorted(x["d"] for x in tr if x["d"])
    print(f"holdings {len(h)}, trades {len(tr)} since {dates[0] if dates else '?'}", flush=True)
    if len(h) < 50:
        sys.exit("Holdings table not read correctly; nothing loaded.")

    ords = httpx.Client(timeout=300)
    tok = ords.post(f"{ORDS_BASE}/oauth/token", data={"grant_type": "client_credentials"},
                    auth=(os.environ["ORDS_CLIENT_ID"], os.environ["ORDS_CLIENT_SECRET"]))
    tok.raise_for_status()
    ords.headers["Authorization"] = "Bearer " + tok.json()["access_token"]
    body = {"holdings": h, "trades": tr, "since": dates[0] if dates else None}
    r = ords.post(f"{ORDS_BASE}/loader/ark", content=json.dumps(body), headers={"Content-Type": "application/json"})
    r.raise_for_status()
    res = r.json()
    print(json.dumps(res, ensure_ascii=False), flush=True)
    if res.get("status") != "OK":
        sys.exit(1)


if __name__ == "__main__":
    main()
