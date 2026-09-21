"""
Diagnostic (manual run): why is every TradingView call ~20 seconds?
Times the same calls two ways: through the MCP Python client, and as plain JSON-RPC over HTTP.
Uses (and safely rotates) the token stored in the DB, like load_news.py.
"""
import asyncio
import json
import time

import httpx

import load_news as ln

SYMBOL = "NASDAQ:AAPL"


async def via_mcp(token):
    headers = {"Authorization": f"Bearer {token.access_token}"}
    async with httpx.AsyncClient(headers=headers, timeout=httpx.Timeout(60, read=300)) as client:
        t = time.time()
        async with ln.streamable_http_client(ln.TV_MCP_URL, http_client=client) as streams:
            async with ln.ClientSession(streams[0], streams[1]) as session:
                await session.initialize()
                tools = [x.name for x in (await session.list_tools()).tools]
                print(f"MCP client: connect+initialize+list_tools {time.time() - t:.1f}s", flush=True)
                name = next(x for x in tools if x.replace("-", "_").endswith("get_news"))
                story = next(x for x in tools if x.replace("-", "_").endswith("get_news_story"))
                search = next(x for x in tools if x.replace("-", "_").endswith("search_symbols"))

                async def timed(label, tool, args):
                    t = time.time()
                    res = await session.call_tool(tool, args)
                    print(f"MCP client: {label} {time.time() - t:.1f}s", flush=True)
                    return res

                for i in range(3):
                    await timed(f"get_news limit 5 #{i + 1}", name, {"symbol": SYMBOL, "limit": 5})
                res = await timed("get_news limit 25 offset 0", name, {"symbol": SYMBOL, "limit": 25, "offset": 0})
                first_id = json.loads("".join(getattr(c, "text", "") for c in res.content))["data"]["headlines"][0]["id"]
                for sym in ["NASDAQ:MSFT", "NYSE:XOM", "NASDAQ:AMD"]:
                    await timed(f"get_news limit 25 {sym}", name, {"symbol": sym, "limit": 25, "offset": 0})
                await timed("get_news_story", story, {"id": first_id})
                await timed("search_symbols", search, {"query": "ATROB", "type_filter": "stock"})
                time.sleep(0.6)   # like a blocking database call inside the loop (load_news.py does this)
                await timed("get_news after a blocking pause", name, {"symbol": SYMBOL, "limit": 25, "offset": 0})
                time.sleep(0.6)
                await timed("get_news_story after a blocking pause", story, {"id": first_id})
                return name


def via_raw(token, tool_name):
    h = {"Authorization": f"Bearer {token.access_token}", "Content-Type": "application/json",
         "Accept": "application/json, text/event-stream"}
    c = httpx.Client(timeout=120)

    def rpc(method, params, rid, extra=None):
        t = time.time()
        r = c.post(ln.TV_MCP_URL, headers={**h, **(extra or {})},
                   content=json.dumps({"jsonrpc": "2.0", "id": rid, "method": method, "params": params}))
        return r, time.time() - t

    r, dt = rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                               "clientInfo": {"name": "diag", "version": "1"}}, 1)
    sid = r.headers.get("mcp-session-id")
    print(f"RAW: initialize {dt:.1f}s status {r.status_code} type {r.headers.get('content-type')} session {bool(sid)}",
          flush=True)
    extra = {"mcp-session-id": sid} if sid else {}
    c.post(ln.TV_MCP_URL, headers={**h, **extra},
           content=json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}))
    for i in range(3):
        r, dt = rpc("tools/call", {"name": tool_name, "arguments": {"symbol": SYMBOL, "limit": 5}}, 10 + i, extra)
        print(f"RAW: get_news #{i + 1} {dt:.1f}s status {r.status_code} type {r.headers.get('content-type')} "
              f"bytes {len(r.content)}", flush=True)


def main():
    ords = ln.Ords()
    token = ln.TvToken(ords)
    t = time.time()
    token.refresh()
    print(f"token refresh {time.time() - t:.1f}s", flush=True)
    name = asyncio.run(via_mcp(token))
    if not ln.GEMINI_API_KEY:
        print("GEMINI: GEMINI_API_KEY is not set in this workflow", flush=True)
    else:
        g = ln.Gemini()
        t = time.time()
        r = g.client.get(f"{ln.GEMINI_URL}/models")
        print(f"GEMINI: list models {time.time() - t:.1f}s status {r.status_code} {r.text[:200] if r.status_code != 200 else ''}",
              flush=True)


if __name__ == "__main__":
    main()
