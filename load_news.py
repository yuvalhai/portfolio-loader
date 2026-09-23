"""
Portfolio news loader (GitHub Actions).
1. Reads the TradingView refresh token from the DB (ORDS /loader/oauth_token/TRADINGVIEW).
2. Refreshes it and saves the new one immediately (TradingView rotates refresh tokens).
3. For every held/watched stock: fills a missing TV_SYMBOL, pulls new headlines from the
   TradingView MCP server and loads them to PF_NEWS_INBOX (stage 1, title rules run in the DB).
4. For headlines still KEEP/REVIEW without a body: pulls the story text and runs stage 2 (body rules).
5. Logs the run to PF_LOAD_RUN (RUN_TYPE NEWS).
6. Stage 3: sends each KEEP/REVIEW item with a body to Gemini and stores its proposal in PF_NEWS_AI,
   together with a Hebrew factual summary of the story (body_he) that the external auditor reads
   instead of the full story, because the MCP connector truncates anything over 4000 bytes.
   Two tiers, newest first: DEEP (good models, first) on the categories that need understanding and on
   escalations; LIGHT (cheap models) on the rest. A LIGHT decision other than DISCARD/ANALYST escalates.
Secrets: ORDS_BASE, ORDS_CLIENT_ID, ORDS_CLIENT_SECRET, GEMINI_API_KEY (optional)
"""
import asyncio
import json
import os
import re
import sys
import threading
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

STOCKS_PER_RUN = 50           # rotation: each run takes the stocks scanned longest ago (held first)
FIRST_RUN_DAYS = 2            # look-back for a stock with no news in the DB yet
HEAD_BUDGET_SEC = 15 * 60     # safety net only: pass 1 (headlines) must end by then
RUN_BUDGET_SEC = 25 * 60      # safety net only: pass 2 (story bodies) stops here; the rest waits for the next run
OVERLAP_SEC = 86400           # re-read one day before the last known headline (duplicates are ignored by the DB)
PAGE = 25
MAX_HEADLINES = 200
MIN_CALL_GAP = 0.7            # TradingView limit is ~100 calls/minute
WORKERS = 6                   # TradingView calls in flight at the same time
MAX_RECONNECTS = 5            # a broken TradingView connection is reopened this many times per run
SESSION_MAX_SEC = 12 * 60     # access token lives 15 minutes; renew before that
US_EXCHANGES = ["NASDAQ", "NYSE", "AMEX", "CBOE", "OTC"]

# stage 3 by Gemini (skipped when GEMINI_API_KEY is not set)
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
# Two tiers (PF_LOGIC 'News AI tiers'). DEEP runs first, on the items that almost surely need
# understanding and on what the LIGHT model escalated; when its daily free quota ends, the rest waits
# for a later run. LIGHT then handles everything else. Best model first in each list.
# Override with GEMINI_MODELS_DEEP / GEMINI_MODELS_LIGHT="a,b,c".
def _models(env, default):
    return [m.strip() for m in (os.environ.get(env) or default).split(",") if m.strip()]


GEMINI_MODELS_DEEP = _models("GEMINI_MODELS_DEEP", "gemini-3-flash-preview,gemini-flash-latest")
GEMINI_MODELS_LIGHT = _models("GEMINI_MODELS_LIGHT", "gemini-3.1-flash-lite,gemini-flash-lite-latest,gemma-4-26b-a4b-it")
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta"
AI_BATCH = 50                 # items per queue request
AI_BUDGET_SEC = 12 * 60       # stage 3 time budget per run, both tiers together
AI_DEEP_MAX_SEC = 6 * 60      # DEEP may use at most this much of it, so LIGHT always gets a turn
AI_BUSY_PAUSE_SEC = 30        # all models of a tier busy on an item -> skip it and wait this long
AI_BUSY_STOP = 3              # ...and stop the tier after this many such items in a row
AI_CALL_GAP = 4.5             # stay well under the free-tier requests-per-minute limit
DECISIONS = {"NEW_CATALYST", "CONFIRMS", "REFUTES", "STANDALONE", "ANALYST", "DISCARD", "ASK"}


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


# ---------------- ORDS ----------------
class Ords:
    def __init__(self):
        self.client = httpx.Client(timeout=120)
        self.lock = threading.Lock()
        r = self.client.post(f"{ORDS_BASE}/oauth/token", data={"grant_type": "client_credentials"},
                             auth=(ORDS_CLIENT_ID, ORDS_CLIENT_SECRET))
        r.raise_for_status()
        self.client.headers["Authorization"] = "Bearer " + r.json()["access_token"]

    def get(self, path):
        r = self.client.get(f"{ORDS_BASE}/loader/{path}")
        r.raise_for_status()
        return r.json()

    def post(self, path, body):
        with self.lock:   # called from several worker threads
            return self._post(path, body)

    def _post(self, path, body):
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
        self.lock = asyncio.Lock()

    def tool(self, suffix):
        name = next((t for t in self.tools if t.replace("-", "_").endswith(suffix)), None)
        if not name:
            raise SystemExit(f"TradingView tool not found: {suffix}")
        return name

    async def call(self, suffix, args):
        async with self.lock:                      # start calls at most every MIN_CALL_GAP seconds (all workers)
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
    """Tries the given models in order. Busy (503) -> next model for this item;
    no quota (429) or not available (404) -> that model is skipped for the rest of the run."""

    def __init__(self, models):
        self.client = httpx.Client(timeout=120, headers={"x-goog-api-key": GEMINI_API_KEY})
        self.models = list(models)
        self.dead = set()
        self.model = self.models[0]
        self.last = 0.0

    def _body(self, model, instructions, item):
        text = json.dumps(item, ensure_ascii=False)
        if model.startswith("gemma"):   # Gemma: no system instruction and no JSON mode
            return {"contents": [{"role": "user", "parts": [{"text": instructions + "\n\nINPUT:\n" + text
                                                             + "\n\nAnswer with the JSON object only."}]}],
                    "generationConfig": {"temperature": 0.2}}
        return {"system_instruction": {"parts": [{"text": instructions}]},
                "contents": [{"role": "user", "parts": [{"text": text}]}],
                "generationConfig": {"responseMimeType": "application/json", "temperature": 0.2}}

    @staticmethod
    def _parse(data):
        text = "".join(p.get("text", "") for c in data.get("candidates", [])[:1]
                       for p in c.get("content", {}).get("parts", []))
        m = re.search(r"\{.*\}", text, re.S)
        res = json.loads(m.group(0) if m else text)
        if isinstance(res, list):
            res = res[0]
        if str(res.get("decision", "")).upper() not in DECISIONS:
            raise RuntimeError(f"bad decision: {text[:200]}")
        # the auditor reads this instead of the story, so it must be there and must fit the column
        res["body_he"] = (res.get("body_he") or "")[:2400]
        return res

    def ask(self, instructions, item):
        errors = []
        for model in [m for m in self.models if m not in self.dead]:
            for attempt in range(2):
                wait = AI_CALL_GAP - (time.time() - self.last)
                if wait > 0:
                    time.sleep(wait)
                self.last = time.time()
                r = self.client.post(f"{GEMINI_URL}/models/{model}:generateContent",
                                     json=self._body(model, instructions, item))
                if r.status_code == 200:
                    if model != self.model:
                        log(f"Gemini model now: {model}")
                        self.model = model
                    return self._parse(r.json())
                errors.append(f"{model} {r.status_code}")
                if r.status_code in (404, 429):
                    self.dead.add(model)
                    log(f"Gemini {model}: {r.status_code} - skipped for this run")
                    break
                if r.status_code in (500, 503):
                    log(f"Gemini {model}: {r.status_code} busy (attempt {attempt + 1})")
                if r.status_code in (500, 503) and attempt == 0:
                    time.sleep(5)
                    continue
                if r.status_code in (500, 503):
                    break
                r.raise_for_status()
        # every model is out of quota or missing -> nothing more this run; otherwise some were only busy
        if all(m in self.dead for m in self.models):
            raise QuotaError("no Gemini model answered: " + ", ".join(errors[-6:]))
        raise BusyError("all Gemini models busy: " + ", ".join(errors[-6:]))


class QuotaError(Exception):
    """All models of the tier are out of quota (429) or unavailable (404): stop the tier for this run."""
    pass


class BusyError(Exception):
    """The models that still have quota are all busy (500/503) right now: skip the item, pause, go on."""
    pass


def run_ai(ords, stats, deadline, tier, models):
    """One tier of stage 3. The DB decides which items belong to the tier (newest first).
    A LIGHT decision other than DISCARD or ANALYST comes back in the DEEP queue of a later run."""
    if not GEMINI_API_KEY:
        log("GEMINI_API_KEY not set - stage 3 skipped")
        return
    gem = Gemini(models)
    done_ids = set()
    n = 0
    busy_streak = 0
    while time.time() < deadline:
        q = ords.get(f"news/ai_queue?limit={AI_BATCH}&tier={tier}")
        items = [i for i in q.get("items") or [] if i["inbox_id"] not in done_ids]
        if not items:
            break
        for item in items:
            if time.time() > deadline:
                break
            done_ids.add(item["inbox_id"])
            try:
                res = gem.ask(q["instructions"], item)
                busy_streak = 0
                check(ords.post("news/ai_result", {"inbox_id": item["inbox_id"], "model": gem.model,
                                                   "tier": tier, "result": res}), "news/ai_result")
                n += 1
                stats["ai"] += 1
                stats["ai_" + tier.lower()] += 1
                if not res["body_he"]:
                    stats["no_summary"] += 1
                if tier == "LIGHT" and str(res.get("decision", "")).upper() not in ("DISCARD", "ANALYST"):
                    stats["escalated"] += 1
            except QuotaError as e:
                if tier == "DEEP":
                    # expected: the good models' daily free quota is small; their queue waits for a later run
                    stats["notes"].append("DEEP quota used up, the rest waits")
                    log(f"stage 3 {tier}: quota used up - {e}")
                else:
                    stats["failed"].append(f"AI {tier} stopped: {e}")
                break
            except BusyError as e:
                # a busy spell at Google is temporary: the item stays in the queue for a later run
                busy_streak += 1
                stats["busy_skipped"] += 1
                log(f"stage 3 {tier}: item {item['inbox_id']} skipped, models busy ({busy_streak} in a row)")
                if busy_streak >= AI_BUSY_STOP:
                    stats["failed"].append(f"AI {tier} stopped: models busy on {busy_streak} items in a row")
                    break
                time.sleep(AI_BUSY_PAUSE_SEC)
            except Exception as e:
                stats["failed"].append(f"AI {tier} {item.get('symbol')} {item['inbox_id']}: {str(e)[:150]}")
        else:
            continue
        break
    log(f"stage 3 {tier}: {n} items decided, last model {gem.model}")


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
        check(await asyncio.to_thread(ords.post, "news/tv", {"id": s["id"], "tv": s["tv"]}), "news/tv")
        stats["tv_filled"] += 1
    items = [to_item(h) for h in await tv.headlines(s["tv"], s.get("since"))]
    res = check(await asyncio.to_thread(ords.post, "news/load", {"id": s["id"], "items": items}), "news/load")
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
    check(await asyncio.to_thread(ords.post, "news/body", {"news_id": job.news_id, "text": text}), "news/body")
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

                async def worker():
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

                # TradingView answers slowly at times (up to ~30 s per call); several calls in flight keep the pace
                await asyncio.gather(*(worker() for _ in range(WORKERS)))


def leaf_errors(e):
    """Readable text of an exception, unwrapping exception groups (the MCP transport raises those)."""
    if isinstance(e, BaseExceptionGroup):
        return "; ".join(leaf_errors(x) for x in e.exceptions)
    return f"{type(e).__name__}: {str(e)[:200]}"


def run_queue(ords, token, queue, bodies, stats, deadline):
    """Runs the queue; when the TradingView connection breaks, reconnects and continues (up to MAX_RECONNECTS)."""
    breaks = 0
    while queue and time.time() < deadline:
        if token.expiring():
            token.refresh()
        try:
            asyncio.run(process(ords, token, queue, bodies, stats, deadline))
        except Exception as e:
            breaks += 1
            err = leaf_errors(e)
            log(f"TradingView connection broke ({breaks}): {err}")
            stats["failed"].append(f"connection break: {err}")
            if breaks >= MAX_RECONNECTS:
                raise RuntimeError(f"TradingView connection broke {breaks} times; last: {err}")
            time.sleep(10)


def main():
    t0 = time.time()
    ords = Ords()
    stats = {"headlines": 0, "new": 0, "bodies": 0, "tv_filled": 0, "failed": [], "t_story": 0.0, "t_ords": 0.0,
             "ai": 0, "ai_deep": 0, "ai_light": 0, "escalated": 0, "no_summary": 0, "busy_skipped": 0, "notes": []}
    status, message = "OK", ""
    bodies = []
    try:
        token = TvToken(ords)
        token.refresh()
        stocks = ords.get(f"news/symbols?limit={STOCKS_PER_RUN}")
        log(f"{len(stocks)} stocks")
        heads = [Job("HEAD", s) for s in stocks]
        run_queue(ords, token, heads, bodies, stats, t0 + HEAD_BUDGET_SEC)
        if heads:
            message += f"Headlines not reached for {len(heads)} stocks (time budget). "
        log(f"headlines done: {stats['headlines']} read, {stats['new']} new, {len(bodies)} bodies to fetch")
        run_queue(ords, token, bodies, bodies, stats, t0 + RUN_BUDGET_SEC)
        if bodies:
            message += f"{len(bodies)} bodies left for the next run. "
        ai_start = time.time()
        run_ai(ords, stats, ai_start + min(AI_DEEP_MAX_SEC, AI_BUDGET_SEC), "DEEP", GEMINI_MODELS_DEEP)
        run_ai(ords, stats, ai_start + AI_BUDGET_SEC, "LIGHT", GEMINI_MODELS_LIGHT)
        if stats["notes"]:
            message += " ".join(stats["notes"]) + ". "
    except SystemExit as e:
        status, message = "ERROR", str(e)
    except Exception as e:
        status, message = "ERROR", leaf_errors(e)[:500]
    if status == "OK" and stats["failed"]:
        status = "PARTIAL"
    n = max(stats["bodies"], 1)
    summary = (f"headlines {stats['headlines']}, new {stats['new']}, bodies {stats['bodies']} "
               f"(avg TradingView {stats['t_story'] / n:.1f}s, DB {stats['t_ords'] / n:.1f}s), "
               f"tv filled {stats['tv_filled']}, AI decided {stats['ai']} "
               f"(deep {stats['ai_deep']}, light {stats['ai_light']}, escalated {stats['escalated']}, "
               f"busy skipped {stats['busy_skipped']}, no summary {stats['no_summary']}), "
               f"{int(time.time() - t0)}s. {message}").strip()
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
