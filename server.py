"""RSI Монитор — сервер (FastAPI).

Отдаёт страницу index.html и живые данные из TradingView scanner:
топ-100 монет по капитализации + RSI 4h их USDT-пар на Binance.

Вход по логину/паролю:
  - логин и пароль берутся из переменных окружения RSI_LOGIN / RSI_PASSWORD,
    а если их нет — из файла auth.json рядом с сервером:  {"login": "...", "password": "..."}
  - auth.json исключён из git (.gitignore) и в публичный репозиторий не попадает;
  - если ни переменных, ни файла нет — сервер работает БЕЗ входа и предупреждает в консоли.

Запуск:  python server.py  ->  http://127.0.0.1:8080
"""
import json
import os
import secrets
import time
import urllib.request
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import (FileResponse, HTMLResponse, JSONResponse,
                               RedirectResponse)

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


# ---------------------------------------------------------------- авторизация

def _load_auth():
    login = os.environ.get("RSI_LOGIN")
    password = os.environ.get("RSI_PASSWORD")
    if login and password:
        return {"login": login, "password": password}
    f = BASE_DIR / "auth.json"
    if f.exists():
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
            if d.get("login") and d.get("password"):
                return {"login": str(d["login"]), "password": str(d["password"])}
        except Exception as e:
            print("auth.json не прочитан:", e)
    return None


AUTH = _load_auth()
if AUTH is None:
    print("ВНИМАНИЕ: логин/пароль не заданы (auth.json или RSI_LOGIN/RSI_PASSWORD) — сайт открыт без входа")

SESSIONS = set()
COOKIE = "rsi_session"

LOGIN_HTML = """<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>RSI Монитор — вход</title>
<style>
  * { margin:0; padding:0; box-sizing:border-box; }
  body { background:#1b1b1f; color:#e6e6e9; font:15px/1.5 "Segoe UI", system-ui, sans-serif;
         min-height:100vh; display:flex; align-items:center; justify-content:center; }
  .card { background:#232327; border:1px solid #2c2c33; border-radius:12px; padding:28px 30px; width:320px; }
  h1 { font-size:18px; font-weight:600; margin-bottom:4px; }
  .sub { color:#8a8a94; font-size:13px; margin-bottom:18px; }
  label { display:block; font-size:13px; color:#a8a8b0; margin:10px 0 5px; }
  input { width:100%; background:#1b1b1f; border:1px solid #3a3a42; color:#e6e6e9;
          border-radius:8px; padding:9px 12px; font-size:14px; }
  button { width:100%; margin-top:18px; background:#4f9cf7; border:none; color:#10131a;
           font-weight:600; border-radius:8px; padding:10px; font-size:14px; cursor:pointer; }
  button:hover { background:#6badf9; }
  .err { color:#f09090; font-size:13px; margin-top:10px; min-height:18px; }
</style></head>
<body>
<form class="card" id="f">
  <h1>◭ RSI Монитор</h1>
  <div class="sub">Вход в систему</div>
  <label>Логин</label>
  <input id="l" autocomplete="username" autofocus>
  <label>Пароль</label>
  <input id="p" type="password" autocomplete="current-password">
  <button type="submit">Войти</button>
  <div class="err" id="e"></div>
</form>
<script>
document.getElementById("f").addEventListener("submit", async ev => {
  ev.preventDefault();
  const r = await fetch("/login", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({login: document.getElementById("l").value,
                          password: document.getElementById("p").value})
  });
  if (r.ok) { window.location.href = "/"; }
  else { document.getElementById("e").textContent = "Неверный логин или пароль"; }
});
</script>
</body></html>"""


# ---------------------------------------------------------------- данные

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


# ---------------------------------------------------------------- приложение

app = FastAPI(title="RSI Монитор")


@app.middleware("http")
async def auth_guard(request: Request, call_next):
    path = request.url.path
    if AUTH is None or path == "/login" or path == "/favicon.ico":
        return await call_next(request)
    token = request.cookies.get(COOKIE)
    if token and token in SESSIONS:
        return await call_next(request)
    if path.startswith("/api"):
        return JSONResponse({"error": "нужен вход"}, status_code=401)
    return RedirectResponse("/login")


@app.get("/login")
def login_page(request: Request):
    token = request.cookies.get(COOKIE)
    if AUTH is None or (token and token in SESSIONS):
        return RedirectResponse("/")
    return HTMLResponse(LOGIN_HTML)


@app.post("/login")
async def login_post(request: Request):
    try:
        data = await request.json()
    except Exception:
        data = {}
    if AUTH and \
       secrets.compare_digest(str(data.get("login", "")), AUTH["login"]) and \
       secrets.compare_digest(str(data.get("password", "")), AUTH["password"]):
        token = secrets.token_urlsafe(32)
        SESSIONS.add(token)
        resp = JSONResponse({"ok": True})
        resp.set_cookie(COOKIE, token, max_age=30 * 24 * 3600,
                        httponly=True, samesite="lax")
        return resp
    return JSONResponse({"ok": False}, status_code=401)


@app.get("/logout")
def logout(request: Request):
    SESSIONS.discard(request.cookies.get(COOKIE))
    resp = RedirectResponse("/login")
    resp.delete_cookie(COOKIE)
    return resp


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
