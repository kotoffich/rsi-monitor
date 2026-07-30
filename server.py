"""RSI Монитор — сервер (FastAPI).

Отдаёт страницу index.html и живые данные из TradingView scanner:
топ-100 USDT-пар Binance по капитализации + RSI 4h.

Запуск:  python server.py  ->  http://127.0.0.1:8080
"""
import json
import time
import urllib.request
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse

BASE_DIR = Path(__file__).parent
COIN_SCAN_URL = "https://scanner.tradingview.com/coin/scan"    # рейтинг монет
PAIR_SCAN_URL = "https://scanner.tradingview.com/crypto/scan"  # данные пар Binance
CACHE_TTL = 60          # сек; чаще TradingView не дёргаем
TOP_N = 100
RANK_POOL = 200         # сколько монет рейтинга берём с запасом (не все есть на Binance)

# базы, которые не считаем "монетами": стейблы и обёрнутые активы
EXCLUDED_BASES = {
    "USDT", "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDP", "PYUSD",
    "USDE", "USD1", "USDS", "XUSD", "AEUR", "EUR",
    "WBTC", "WBETH", "WETH", "STETH", "WSTETH", "CBBTC",
}

_cache = {"ts": 0.0, "data": None}


def _tv_request(url: str, payload: dict) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read())


def fetch_top100() -> dict:
    # Шаг 1: рейтинг настоящих монет по капитализации (без токенизированных акций)
    rank_raw = _tv_request(COIN_SCAN_URL, {
        "columns": ["base_currency", "crypto_total_rank", "market_cap_calc"],
        "sort": {"sortBy": "crypto_total_rank", "sortOrder": "asc"},
        "range": [0, RANK_POOL],
    })
    bases, mcap_by_base = [], {}
    for row in rank_raw.get("data", []):
        base, _rank, mcap = row["d"]
        if not base or base in EXCLUDED_BASES or base in mcap_by_base:
            continue
        bases.append(base)
        mcap_by_base[base] = mcap

    # Шаг 2: данные USDT-пар Binance по этому списку (несуществующие пары отпадут сами)
    tickers = ["BINANCE:" + b + "USDT" for b in bases]
    pair_raw = _tv_request(PAIR_SCAN_URL, {
        "columns": ["name", "close", "change", "RSI|240"],
        "symbols": {"query": {"types": []}, "tickers": tickers},
    })
    by_name = {}
    for row in pair_raw.get("data", []):
        name, close, change, rsi = row["d"]
        if name:
            by_name[name] = (close, change, rsi)

    coins = []
    for base in bases:
        data = by_name.get(base + "USDT")
        if not data:
            continue
        close, change, rsi = data
        if close is None or rsi is None:
            continue
        coins.append({
            "sym": base + "USDT",
            "base": base,
            "price": close,
            "change24h": round(change, 2) if change is not None else None,
            "rsi4h": round(rsi, 1),
            "mcap": mcap_by_base.get(base),
        })
        if len(coins) >= TOP_N:
            break

    return {
        "updated": time.strftime("%H:%M:%S"),
        "source": "TradingView (scanner)",
        "count": len(coins),
        "coins": coins,
    }


app = FastAPI(title="RSI Монитор")


@app.get("/")
def index():
    return FileResponse(BASE_DIR / "index.html")


@app.get("/api/scan")
def api_scan(force: int = 0):
    now = time.time()
    if not force and _cache["data"] and now - _cache["ts"] < CACHE_TTL:
        return JSONResponse(_cache["data"])
    try:
        data = fetch_top100()
    except Exception as e:
        if _cache["data"]:
            stale = dict(_cache["data"])
            stale["error"] = f"TradingView недоступен: {e}"
            return JSONResponse(stale)
        return JSONResponse({"error": str(e), "coins": []}, status_code=502)
    _cache["data"] = data
    _cache["ts"] = now
    return JSONResponse(data)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8080)
