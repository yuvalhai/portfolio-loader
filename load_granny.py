"""
Weekly: holdings of Tom Lee's Fundstrat Granny Shots ETFs (GRNY, GRNJ, GRNI) -> recommendations of 'Granny Shots'.
Reads the holdings table of each fund page and posts it to ORDS /loader/etf_holdings.
Secrets: ORDS_BASE, ORDS_CLIENT_ID, ORDS_CLIENT_SECRET
"""
import json
import os
import re
import sys
from datetime import datetime
from html.parser import HTMLParser

import httpx

ORDS_BASE = os.environ["ORDS_BASE"].rstrip("/")
FUNDS = [
    ("GRNY", "https://grannyshots.com/fundstrat-granny-shots-us-large-cap-etf/grny-holdings/"),
    ("GRNJ", "https://grannyshots.com/fundstrat-granny-shots-us-small-mid-cap-etf/grnj-holdings/"),
    ("GRNI", "https://grannyshots.com/fundstrat-granny-shots-us-large-cap-and-income-etf/grni-holdings/"),
]
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/128.0 Safari/537.36",
           "Accept": "text/html,application/xhtml+xml"}
TICKER_RE = re.compile(r"^[A-Z]{1,5}(\.[A-Z])?$")


class Tables(HTMLParser):
    """Collects every table as a list of rows (list of cell texts)."""
    def __init__(self):
        super().__init__()
        self.tables, self.row, self.cell = [], None, None

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self.tables.append([])
        elif tag == "tr" and self.tables:
            self.row = []
        elif tag in ("td", "th") and self.row is not None:
            self.cell = []

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self.cell is not None:
            self.row.append(" ".join("".join(self.cell).split()))
            self.cell = None
        elif tag == "tr" and self.row is not None:
            if self.row:
                self.tables[-1].append(self.row)
            self.row = None

    def handle_data(self, data):
        if self.cell is not None:
            self.cell.append(data)


def norm_ticker(t):
    t = t.strip().upper().replace("/", ".").replace(" ", ".").replace("-", ".")
    return t if TICKER_RE.match(t) else None


def parse(html):
    p = Tables()
    p.feed(html)
    for table in p.tables:
        if not table:
            continue
        head = [h.lower() for h in table[0]]
        if "ticker" not in head or not any("weight" in h for h in head):
            continue
        it, iw = head.index("ticker"), next(i for i, h in enumerate(head) if "weight" in h)
        ic = head.index("cusip") if "cusip" in head else None
        inm = head.index("name") if "name" in head else None
        out = []
        for row in table[1:]:
            if len(row) <= max(it, iw):
                continue
            t = norm_ticker(row[it])
            if not t or (ic is not None and len(re.sub(r"[^0-9A-Z]", "", row[ic].upper())) != 9):
                continue  # cash, futures, options and other non-stock lines
            try:
                w = float(row[iw].replace("%", "").replace(",", ""))
            except ValueError:
                continue
            out.append({"t": t, "n": row[inm].strip('"') if inm is not None else None, "w": w})
        if out:
            return out
    return []


def asof(html):
    m = re.search(r"Holdings as of\s+([A-Z][a-z]+ \d{1,2}, \d{4})", html)
    if m:
        return datetime.strptime(m.group(1), "%B %d, %Y").strftime("%Y-%m-%d")
    return None


def main():
    funds, dates, problems = [], [], []
    with httpx.Client(headers=HEADERS, timeout=60, follow_redirects=True) as web:
        for fund, url in FUNDS:
            try:
                r = web.get(url)
                r.raise_for_status()
                hold = parse(r.text)
                if not hold:
                    problems.append(f"{fund}: no holdings table found")
                    continue
                funds.append({"fund": fund, "url": url, "holdings": hold})
                if asof(r.text):
                    dates.append(asof(r.text))
                print(f"{fund}: {len(hold)} holdings, as of {asof(r.text)}", flush=True)
            except Exception as e:
                problems.append(f"{fund}: {type(e).__name__} {str(e)[:200]}")
    for p in problems:
        print("PROBLEM " + p, flush=True)
    if len(funds) < len(FUNDS):
        # a missing fund would expire its stocks by mistake - load nothing
        sys.exit("Not all funds were read; nothing loaded.")

    ords = httpx.Client(timeout=300)
    tok = ords.post(f"{ORDS_BASE}/oauth/token", data={"grant_type": "client_credentials"},
                    auth=(os.environ["ORDS_CLIENT_ID"], os.environ["ORDS_CLIENT_SECRET"]))
    tok.raise_for_status()
    ords.headers["Authorization"] = "Bearer " + tok.json()["access_token"]
    body = {"asof": max(dates) if dates else None, "funds": funds}
    r = ords.post(f"{ORDS_BASE}/loader/etf_holdings", content=json.dumps(body),
                  headers={"Content-Type": "application/json"})
    r.raise_for_status()
    res = r.json()
    print(json.dumps(res, ensure_ascii=False), flush=True)
    if res.get("status") != "OK":
        sys.exit(1)


if __name__ == "__main__":
    main()
