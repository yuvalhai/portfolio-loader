"""Bounded retry for the twelve financial-history issues diagnosed on 2026-10-03."""
import asyncio
import json
import httpx
import load_news as ln

TARGETS = [(2521,"NYSE:ALLY"),(249,"NASDAQ:AMAT"),(415,"NYSE:AMPX"),(276,"NYSE:B"),(359,"NYSE:CARR"),(333,"NYSE:CB"),(363,"NYSE:CC"),(2586,"NASDAQ:CDNA"),(328,"NASDAQ:CDNS"),(343,"NYSE:CF"),(355,"NYSE:CFR"),(88,"NASDAQ:CGNX")]

async def repair(ords, token):
    headers = {"Authorization": f"Bearer {token.access_token}"}
    async with httpx.AsyncClient(headers=headers, timeout=httpx.Timeout(60, read=120)) as client:
        async with ln.streamable_http_client(ln.TV_MCP_URL, http_client=client) as streams:
            async with ln.ClientSession(streams[0], streams[1]) as session:
                await session.initialize()
                tv = ln.Tv(session, [t.name for t in (await session.list_tools()).tools])
                for security_id, symbol in TARGETS:
                    try:
                        fq = await tv.fin_history(symbol, "fq", ln.FIN_HISTORY_QUARTERS_DAYS)
                        fy = await tv.fin_history(symbol, "fy", ln.FIN_HISTORY_YEARS_DAYS)
                        if not any(isinstance(p,dict) and p.get("labels") and p.get("series") for p in (fq,fy)):
                            print("REPAIR_EMPTY", symbol, flush=True)
                            continue
                        result = ln.check(ords.post("security/fin_history", {"id":security_id,"fq":fq,"fy":fy}), "security/fin_history")
                        print("REPAIR_RESULT", symbol, json.dumps(result), flush=True)
                    except Exception as error:
                        print("REPAIR_ERROR", symbol, str(error)[:300], flush=True)
                        if "429" in str(error):
                            break

if __name__ == "__main__":
    ords = ln.Ords()
    token = ln.TvToken(ords)
    token.refresh()
    asyncio.run(repair(ords, token))
