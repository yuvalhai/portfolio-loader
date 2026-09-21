"""
Portfolio news loader (GitHub Actions).
1. Reads the TradingView refresh token from the DB (ORDS /loader/oauth_token/TRADINGVIEW).
2. Refreshes it and saves the new one immediately (TradingView rotates refresh tokens).
3. For every held/watched stock: fills a missing TV_SYMBOL, pulls new headlines from the
   TradingView MCP server and loads them to PF_NEWS_INBOX (stage 1, title rules run in the DB).
4. For headlines still KEEP/REVIEW without a body: pulls the story text and runs stage 2 (body rules).
5. Logs the run to PF_LOAD_RUN (RUN_TYPE NEWS).
6. Stage 3: sends each KEEP/REVIEW item with a body to Gemini and stores its proposal in PF_NEWS_AI (Claude reviews).
Secrets: ORDS_BASE, ORDS_CLIENT_ID, ORDS_CLIENT_SECRET, GEMINI_API_KEY (optional)
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

FIRST_RUN_DAYS = 2            # look-back for a stock with no news in the DB yet
HEAD_BUDGET_SEC = 15 * 60     # pass 1 (headlines for all stocks) must end by then
RUN_BUDGET_SEC = 25 * 60      # pass 2 (story bodies) stops here; the rest waits for the next run
OVERLAP_SEC = 86400           # re-read one day before the last known headline (duplicates are ignored by the DB)
PAGE = 25
MAX_HEADLINES = 200
MIN_CALL_GAP = 0.7            # TradingView limit is ~100 calls/minute
SESSION_MAX_SEC = 12 * 60     # access token lives 15 minutes; renew before that
US_EXCHANGES = ["NASDAQ", "NYSE", "AMEX", "CBOE", "OTC"]

# stage 3 by Gemini (skipped when GEMINI_API_KEY is not set)
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "").strip() or "gemini-flash-latest"
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta"
AI_BATCH = 50                 # items per queue request
AI_BUDGET_SEC = 12 * 60       # stage 3 time budget per run
AI_CALL_GAP = 4.5             # stay well under the free-tier requests-per-minute limit
DECISIONS = {"NEW_CATALYST", "CONFIRMS", "REFUTES", "STANDALONE", "DISCARD", "ASK"}


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


# ---------------- stage 3: Gemini ----------------
class Gemini:
    def __init__(self):
        self.client = httpx.Client(timeout=120, headers={"x-goog-api-key": GEMINI_API_KEY})
        self.model = GEMINI_MODEL
        self.last = 0.0

    def pick_model(self):
        """Fallback when the configured model name is unknown: newest-looking flash model that can generate."""
        r = self.client.get(f"{GEMINI_URL}/models")
        r.raise_for_status()
        names = [m["name"].split("/", 1)[-1] for m in r.json().get("models", [])
                 if "generateContent" in m.get("supportedGenerationMethods", [])]
        good = [n for n in names if "flash" in n and not re.search(r"lite|image|tts|live|audio|thinking|exp", n)]
        if not good:
            raise RuntimeError(f"No Gemini flash model found in {names[:20]}")
        self.model = sorted(good)[-1]
        log(f"Gemini model: {self.model}")

    def ask(self, instructions, item):
        body = {
            "system_instruction": {"parts": [{"text": instructions}]},
            "contents": [{"role": "user", "parts": [{"text": json.dumps(item, ensure_ascii=False)}]}],
            "generationConfig": {"responseMimeType": "application/json", "temperature": 0.2},
        }
        for attempt in range(4):
            wait = AI_CALL_GAP - (time.time() - self.last)
            if wait > 0:
                time.sleep(wait)
            self.last = time.time()
            r = self.client.post(f"{GEMINI_URL}/models/{self.model}:generateContent", json=body)
            if r.status_code == 404 and attempt == 0:
                self.pick_model()
                continue
            if r.status_code in (429, 500, 503):
                if attempt == 3:
                    raise QuotaError(f"Gemini {r.status_code}: {r.text[:200]}")
                time.sleep(20 * (attempt + 1))
                continue
            r.raise_for_status()
            data = r.json()
            text = "".join(p.get("text", "") for c in data.get("candidates", [])[:1]
                           for p in c.get("content", {}).get("parts", []))
            text = re.sub(r"^```(json)?|```$", "", text.strip()).strip()
            res = json.loads(text)
            if isinstance(res, list):
                res = res[0]
            if str(res.get("decision", "")).upper() not in DECISIONS:
                raise RuntimeError(f"bad decision: {text[:200]}")
            return res
        raise RuntimeError("Gemini: no answer")


class QuotaError(Exception):
    pass


def run_ai(ords, stats, deadline):
    if not GEMINI_API_KEY:
        log("GEMINI_API_KEY not set - stage 3 skipped")
        return
    gem = Gemini()
    done_ids = set()
    while time.time() < deadline:
        q = ords.get(f"news/ai_queue?limit={AI_BATCH}")
        items = [i for i in q.get("items") or [] if i["inbox_id"] not in done_ids]
        if not items:
            break
        for item in items:
            if time.time() > deadline:
                break
            done_ids.add(item["inbox_id"])
            try:
                res = gem.ask(q["instructions"], item)
                check(ords.post("news/ai_result", {"inbox_id": item["inbox_id"], "model": gem.model, "result": res}),
                      "news/ai_result")
                stats["ai"] += 1
            except QuotaError as e:
                stats["failed"].append(f"AI stopped: {e}")
                return
            except Exception as e:
                stats["failed"].append(f"AI {item.get('symbol')} {item['inbox_id']}: {str(e)[:150]}")
    log(f"stage 3: {stats['ai']} items decided by {gem.model}")


# ---------------- main ----------------
class Job:
    """HEAD job: resolve TV symbol + load headlines for one stock. BODY job: story text of one headline."""
    def __init__(self, kind, stock, news_id=None):
        self.kind, self.stock, self.news_id = kind, stock, news_id


def check(res, what):
    if not isinstance(res, dict) or res.get("status") == "ERROR":
        raise RuntimeError(f"{what}: {res}")
    return res


async def do_head(ords, tv, s, stats, bodies):
    if not s.get("tv"):
        s["tv"] = await tv.resolve(s["s"])
        if not s["tv"]:
            stats["failed"].append(f"{s['s']}: TV symbol not found")
            return
        check(ords.post("news/tv", {"id": s["id"], "tv": s["tv"]}), "news/tv")
        stats["tv_filled"] += 1
    items = [to_item(h) for h in await tv.headlines(s["tv"], s.get("since"))]
    res = check(ords.post("news/load", {"id": s["id"], "items": items}), "news/load")
    if (res.get("result") or {}).get("status") != "OK":
        raise RuntimeError(f"news/load: {res}")
    stats["headlines"] += len(items)
    stats["new"] += res["result"].get("new", 0)
    for nid in res.get("need_body") or []:
        bodies.append(Job("BODY", s, nid))


async def do_body(ords, tv, job, stats):
    t1 = time.time()
    text = await tv.story(job.news_id)
    t2 = time.time()
    check(ords.post("news/body", {"news_id": job.news_id, "text": text}), "news/body")
    t3 = time.time()
    stats["bodies"] += 1
    stats["t_story"] += t2 - t1
    stats["t_ords"] += t3 - t2
    if stats["bodies"] <= 3 or stats["bodies"] % 25 == 0:
        log(f"body {stats['bodies']} {job.stock['s']}: TradingView {t2 - t1:.1f}s, DB {t3 - t2:.1f}s")


async def process(ords, token, queue, bodies, stats, deadline):
    """Works through the queue; returns when done, out of time, or when the access token needs renewal."""
    headers = {"Authorization": f"Bearer {token.access_token}"}
    async with httpx.AsyncClient(headers=headers, timeout=httpx.Timeout(60, read=300)) as client:
        async with streamable_http_client(TV_MCP_URL, http_client=client) as streams:
            async with ClientSession(streams[0], streams[1]) as session:
                await session.initialize()
                tv = Tv(session, [t.name for t in (await session.list_tools()).tools])
                while queue:
                    if token.expiring() or time.time() > deadline:
                        return
                    job = queue.pop(0)
                    try:
                        if job.kind == "HEAD":
                            await do_head(ords, tv, job.stock, stats, bodies)
                        else:
                            await do_body(ords, tv, job, stats)
                    except Exception as e:
                        what = job.stock["s"] + (f" body {job.news_id}" if job.news_id else "")
                        stats["failed"].append(f"{what}: {str(e)[:200]}")


def run_queue(ords, token, queue, bodies, stats, deadline):
    while queue and time.time() < deadline:
        if token.expiring():
            token.refresh()
        asyncio.run(process(ords, token, queue, bodies, stats, deadline))


def main():
    t0 = time.time()
    ords = Ords()
    stats = {"headlines": 0, "new": 0, "bodies": 0, "tv_filled": 0, "failed": [], "t_story": 0.0, "t_ords": 0.0, "ai": 0}
    status, message = "OK", ""
    bodies = []
    try:
        token = TvToken(ords)
        token.refresh()
        stocks = ords.get("news/symbols")
        log(f"{len(stocks)} stocks")
        heads = [Job("HEAD", s) for s in stocks]
        run_queue(ords, token, heads, bodies, stats, t0 + HEAD_BUDGET_SEC)
        if heads:
            message += f"Headlines not reached for {len(heads)} stocks (time budget). "
        log(f"headlines done: {stats['headlines']} read, {stats['new']} new, {len(bodies)} bodies to fetch")
        run_queue(ords, token, bodies, bodies, stats, t0 + RUN_BUDGET_SEC)
        if bodies:
            message += f"{len(bodies)} bodies left for the next run. "
        run_ai(ords, stats, time.time() + AI_BUDGET_SEC)
    except SystemExit as e:
        status, message = "ERROR", str(e)
    except Exception as e:
        status, message = "ERROR", f"{type(e).__name__}: {str(e)[:500]}"
    if status == "OK" and stats["failed"]:
        status = "PARTIAL"
    n = max(stats["bodies"], 1)
    summary = (f"headlines {stats['headlines']}, new {stats['new']}, bodies {stats['bodies']} "
               f"(avg TradingView {stats['t_story'] / n:.1f}s, DB {stats['t_ords'] / n:.1f}s), "
               f"tv filled {stats['tv_filled']}, AI decided {stats['ai']}, {int(time.time() - t0)}s. {message}").strip()
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
