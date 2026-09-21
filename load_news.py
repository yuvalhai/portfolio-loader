"""
Portfolio news loader (GitHub Actions).
1. Reads the TradingView refresh token from the DB (ORDS /loader/oauth_token/TRADINGVIEW).
2. Refreshes it and saves the new one immediately (TradingView rotates refresh tokens).
3. For every held/watched stock: fills a missing TV_SYMBOL, pulls new headlines from the
   TradingView MCP server and loads them to PF_NEWS_INBOX (stage 1, title rules run in the DB).
4. For headlines still KEEP/REVIEW without a body: pulls the story text and runs stage 2 (body rules).
5. Logs the run to PF_LOAD_RUN (RUN_TYPE NEWS).
Secrets: ORDS_BASE, ORDS_CLIENT_ID, ORDS_CLIENT_SECRET
"""
import asyncio
import json
import os
import re
import sys
import time

import httpx
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamable_http_client

ORDS_BASE = os.environ["ORDS_BASE"].rstrip("/")
ORDS_CLIENT_ID = os.environ["ORDS_CLIENT_ID"]
ORDS_CLIENT_SECRET = os.environ["ORDS_CLIENT_SECRET"]

TV_MCP_URL = "https://mcp.tradingview.com/mcp"
TV_TOKEN_URL = "https://www.tradingview.com/mcp/oauth/token"
PROVIDER = "TRADINGVIEW"

FIRST_RUN_DAYS = 7            # look-back for a stock with no news in the DB yet
OVERLAP_SEC = 86400           # re-read one day before the last known headline (duplicates are ignored by the DB)
PAGE = 25
MAX_HEADLINES = 200
MIN_CALL_GAP = 0.7            # TradingView limit is ~100 calls/minute
SESSION_MAX_SEC = 12 * 60     # access token lives 15 minutes; renew before that
US_EXCHANGES = ["NASDAQ", "NYSE", "AMEX", "CBOE", "OTC"]


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


# ---------------- ORDS ----------------
class Ords:
    def __init__(self):
        self.client = httpx.Client(timeout=120)
        r = self.client.post(f"{ORDS_BASE}/oauth/token", data={"grant_type": "client_credentials"},
                             auth=(ORDS_CLIENT_ID, ORDS_CLIENT_SECRET))
        r.raise_for_status()
        self.client.headers["Authorization"] = "Bearer " + r.json()["access_token"]

    def get(self, path):
        r = self.client.get(f"{ORDS_BASE}/loader/{path}")
        r.raise_for_status()
        return r.json()

    def post(self, path, body):
        r = self.client.post(f"{ORDS_BASE}/loader/{path}", content=json.dumps(body),
                             headers={"Content-Type": "application/json"})
        r.raise_for_status()
        return r.json()


# ---------------- TradingView token ----------------
class TvToken:
    def __init__(self, ords: Ords):
        self.ords = ords
        row = ords.get(f"oauth_token/{PROVIDER}")
        if "error" in row:
            raise SystemExit(f"No TradingView token in DB: {row}")
        self.client_id = row["client_id"]
        self.client_secret = row.get("client_secret")
        self.refresh_token = row["refresh_token"]
        self.access_token = None
        self.issued = 0.0

    def refresh(self):
        form = {"grant_type": "refresh_token", "refresh_token": self.refresh_token,
                "client_id": self.client_id, "resource": TV_MCP_URL}
        auth = (self.client_id, self.client_secret) if self.client_secret else None
        r = httpx.post(TV_TOKEN_URL, data=form, auth=auth, timeout=60)
        if r.status_code >= 300:
            err = f"TradingView refresh failed {r.status_code}: {r.text[:500]}"
            self.ords.post("oauth_token", {"provider": PROVIDER, "error": err})
            raise SystemExit(err)
        tok = r.json()
        self.access_token = tok["access_token"]
        self.issued = time.time()
        new_rt = tok.get("refresh_token")
        if new_rt and new_rt != self.refresh_token:
            self.refresh_token = new_rt
        # save at once: the old refresh token is no longer valid
        res = self.ords.post("oauth_token", {"provider": PROVIDER, "refresh_token": self.refresh_token})
        if res.get("status") != "OK":
            raise SystemExit(f"Could not save the new refresh token: {res}")
        log("TradingView token refreshed and saved")

    def expiring(self):
        return time.time() - self.issued > SESSION_MAX_SEC


# ---------------- helpers ----------------
def ast_to_text(node):
    if isinstance(node, str):
        return node
    if isinstance(node, list):
        return "".join(ast_to_text(n) for n in node)
    if isinstance(node, dict):
        if node.get("type") == "url":
            return (node.get("params") or {}).get("linkText", "")
        inner = ast_to_text(node.get("children", []))
        return inner + ("\n" if node.get("type") in ("p", "li", "h1", "h2", "h3", "table", "tr") else "")
    return ""


def story_text(story):
    ast = story.get("ast_description")
    if ast:
        try:
            txt = ast_to_text(json.loads(ast) if isinstance(ast, str) else ast).strip()
            if txt:
                return re.sub(r"\n{3,}", "\n\n", txt)
        except (ValueError, TypeError):
            pass
    return (story.get("summary") or story.get("short_description") or "").strip()


def norm(s):
    return re.sub(r"[^A-Z0-9]", "", (s or "").upper())


class Tv:
    def __init__(self, session: ClientSession, tools):
        self.session = session
        self.tools = tools
        self.last = 0.0

    def tool(self, suffix):
        name = next((t for t in self.tools if t.replace("-", "_").endswith(suffix)), None)
        if not name:
            raise SystemExit(f"TradingView tool not found: {suffix}")
        return name

    async def call(self, suffix, args):
        wait = MIN_CALL_GAP - (time.time() - self.last)
        if wait > 0:
            await asyncio.sleep(wait)
        self.last = time.time()
        res = await self.session.call_tool(self.tool(suffix), args)
        text = "".join(getattr(c, "text", "") for c in res.content)
        if res.isError:
            raise RuntimeError(text[:300])
        data = json.loads(text)
        return data.get("data", data) if isinstance(data, dict) else data

    async def resolve(self, symbol):
        data = await self.call("search_symbols", {"query": symbol, "type_filter": "stock"})
        cands = [c for c in data.get("symbols", [])
                 if c.get("exchange") in US_EXCHANGES and norm(c["symbol"].split(":", 1)[-1]) == norm(symbol)]
        cands.sort(key=lambda c: US_EXCHANGES.index(c["exchange"]))
        return cands[0]["symbol"] if cands else None

    async def headlines(self, tv_symbol, since):
        cutoff = (since - OVERLAP_SEC) if since else (time.time() - FIRST_RUN_DAYS * 86400)
        out, offset = [], 0
        while offset < MAX_HEADLINES:
            data = await self.call("get_news", {"symbol": tv_symbol, "limit": PAGE, "offset": offset})
            batch = data.get("headlines", [])
            fresh = [h for h in batch if (h.get("published") or 0) >= cutoff]
            out.extend(fresh)
            if len(fresh) < len(batch) or not data.get("has_more") or not batch:
                break
            offset += PAGE
        return out

    async def story(self, news_id):
        return story_text(await self.call("get_news_story", {"id": news_id}))


def to_item(h):
    return {
        "id": h["id"],
        "t": h.get("title"),
        "p": (h.get("provider") or {}).get("id"),
        "pn": (h.get("provider") or {}).get("name"),
        "ts": h.get("published"),
        "u": h.get("link"),
        "pw": "Y" if h.get("paywall") else "N",
        "pm": h.get("permission"),
        "rs": ",".join(r["symbol"] for r in h.get("relatedSymbols") or [] if r.get("symbol")),
    }


# ---------------- main ----------------
async def process(ords, token, stocks, stats):
    """Works through the queue; returns when done or when the access token needs renewal."""
    headers = {"Authorization": f"Bearer {token.access_token}"}
    async with httpx.AsyncClient(headers=headers, timeout=httpx.Timeout(60, read=300)) as client:
        async with streamable_http_client(TV_MCP_URL, http_client=client) as streams:
            async with ClientSession(streams[0], streams[1]) as session:
                await session.initialize()
                tv = Tv(session, [t.name for t in (await session.list_tools()).tools])
                while stocks:
                    if token.expiring():
                        return
                    s = stocks[0]
                    try:
                        if not s.get("tv"):
                            s["tv"] = await tv.resolve(s["s"])
                            if not s["tv"]:
                                stats["failed"].append(f"{s['s']}: TV symbol not found")
                                stocks.pop(0)
                                continue
                            ords.post("news/tv", {"id": s["id"], "tv": s["tv"]})
                            stats["tv_filled"] += 1
                        if "need_body" not in s:
                            items = [to_item(h) for h in await tv.headlines(s["tv"], s.get("since"))]
                            res = ords.post("news/load", {"id": s["id"], "items": items})
                            if res.get("status") == "ERROR" or (res.get("result") or {}).get("status") != "OK":
                                raise RuntimeError(f"load: {res}")
                            stats["headlines"] += len(items)
                            stats["new"] += res["result"].get("new", 0)
                            s["need_body"] = list(res.get("need_body") or [])
                        while s["need_body"]:
                            if token.expiring():
                                return
                            nid = s["need_body"][0]
                            try:
                                text = await tv.story(nid)
                                ords.post("news/body", {"news_id": nid, "text": text})
                                stats["bodies"] += 1
                            except Exception as e:  # one bad story must not stop the stock
                                stats["failed"].append(f"{s['s']} body {nid}: {str(e)[:150]}")
                            s["need_body"].pop(0)
                    except Exception as e:
                        stats["failed"].append(f"{s['s']}: {str(e)[:200]}")
                    stocks.pop(0)


def main():
    t0 = time.time()
    ords = Ords()
    stats = {"headlines": 0, "new": 0, "bodies": 0, "tv_filled": 0, "failed": []}
    status, message = "OK", ""
    try:
        token = TvToken(ords)
        token.refresh()
        stocks = ords.get("news/symbols")
        log(f"{len(stocks)} stocks")
        while stocks:
            if token.expiring():
                token.refresh()
            asyncio.run(process(ords, token, stocks, stats))
            log(f"{len(stocks)} stocks left")
    except SystemExit as e:
        status, message = "ERROR", str(e)
    except Exception as e:
        status, message = "ERROR", f"{type(e).__name__}: {str(e)[:500]}"
    if status == "OK" and stats["failed"]:
        status = "PARTIAL"
    summary = (f"headlines {stats['headlines']}, new {stats['new']}, bodies {stats['bodies']}, "
               f"tv filled {stats['tv_filled']}, {int(time.time() - t0)}s. {message}").strip()
    log(f"{status}: {summary}")
    for f in stats["failed"]:
        log("FAILED " + f)
    try:
        ords.post("news/run", {"status": status, "rows": stats["new"], "failed": stats["failed"], "message": summary})
    except Exception as e:
        log(f"Could not log the run: {e}")
    sys.exit(1 if status == "ERROR" else 0)


if __name__ == "__main__":
    main()
