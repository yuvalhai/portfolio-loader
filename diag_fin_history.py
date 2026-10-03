"""Read-only financial-history diagnostic for SYNA. Uses the loader's token lifecycle."""
import asyncio
import json
import time
import httpx
import load_news as ln

async def probe(token):
    headers = {"Authorization": f"Bearer {token.access_token}"}
    async with httpx.AsyncClient(headers=headers, timeout=httpx.Timeout(60, read=120)) as client:
        async with ln.streamable_http_client(ln.TV_MCP_URL, http_client=client) as streams:
            async with ln.ClientSession(streams[0], streams[1]) as session:
                await session.initialize()
                tools = (await session.list_tools()).tools
                tool = next(t for t in tools if t.name.replace("-", "_").endswith("get_financial_history"))
                print("FIN_TOOL_SCHEMA", json.dumps(tool.inputSchema), flush=True)
                for period, days in [("fq", ln.FIN_HISTORY_QUARTERS_DAYS), ("fy", ln.FIN_HISTORY_YEARS_DAYS)]:
                    args = {"symbol": "NASDAQ:SYNA", "period": period,
                            "date_from": time.strftime("%Y-%m-%d", time.gmtime(time.time() - days * 86400))}
                    print("FIN_REQUEST", json.dumps(args), flush=True)
                    res = await session.call_tool(tool.name, args)
                    text = "".join(getattr(c, "text", "") for c in res.content)
                    print("FIN_RESPONSE", period, "isError", res.isError, text, flush=True)
                    await asyncio.sleep(ln.SCANNER_GAP)

def main():
    ords = ln.Ords()
    token = ln.TvToken(ords)
    token.refresh()
    asyncio.run(probe(token))

if __name__ == "__main__":
    main()
