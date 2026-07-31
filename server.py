"""RSI Монитор — сервер (FastAPI).

Отдаёт страницу index.html и живые данные из TradingView scanner:
топ-100 монет по капитализации + RSI 4h их USDT-пар на Binance.

Вход по логину/паролю:
  - логин и пароль берутся из переменных окружения RSI_LOGIN / RSI_PASSWORD,
    а если их нет — из файла auth.json рядом с сервером:  {"login": "...", "password": "..."}
  - auth.json исключён из git (.gitignore) и в публичный репозиторий не попадает;
  - если ни переменных, ни файла нет — сервер работает БЕЗ входа и предупреждает в консоли.

Приём сигналов от TradingView через Make (вебхук):
  POST /api/signal?token=ВЕБХУК_ТОКЕН   тело JSON:
  {"symbol":"ADAUSDT","interval":"240","indicator":"div","signal":"bull","price":0.17,"note":"..."}
  Токен — из RSI_WEBHOOK_TOKEN (env) или auth.json["webhook_token"].

Запуск:  python server.py  ->  http://127.0.0.1:8080
"""
import hashlib
import json
import os
import re
import secrets
import time
import urllib.request
from collections import deque
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import (FileResponse, HTMLResponse, JSONResponse,
                               RedirectResponse)

BASE_DIR = Path(__file__).parent
COIN_SCAN_URL = "https://scanner.tradingview.com/coin/scan"    # рейтинг монет
PAIR_SCAN_URL = "https://scanner.tradingview.com/crypto/scan"  # данные пар Binance
CACHE_TTL = 10          # сек; чаще TradingView не дёргаем
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

_auth_file = {}
_af = BASE_DIR / "auth.json"
if _af.exists():
    try:
        _auth_file = json.loads(_af.read_text(encoding="utf-8"))
    except Exception as e:
        print("auth.json не прочитан:", e)


def _load_auth():
    login = os.environ.get("RSI_LOGIN") or _auth_file.get("login")
    password = os.environ.get("RSI_PASSWORD") or _auth_file.get("password")
    if login and password:
        return {"login": str(login), "password": str(password)}
    return None


AUTH = _load_auth()
if AUTH is None:
    print("ВНИМАНИЕ: логин/пароль не заданы (auth.json или RSI_LOGIN/RSI_PASSWORD) — сайт открыт без входа")

# Токен для приёма вебхуков (Make/TradingView шлют сигналы на /api/signal?token=...).
# Берётся из env RSI_WEBHOOK_TOKEN или auth.json["webhook_token"]; иначе — производный от пароля.
WEBHOOK_TOKEN = (
    os.environ.get("RSI_WEBHOOK_TOKEN")
    or _auth_file.get("webhook_token")
    or (hashlib.sha256((AUTH["password"] + ":hook").encode()).hexdigest()[:16] if AUTH else None)
)

def _code_version() -> str:
    """Версия кода: git-коммит (или отпечаток файлов, если git недоступен).

    Токен сессии включает эту версию, поэтому каждое обновление кода
    разлогинивает всех — по ссылке снова запрашивается пароль.
    """
    v = os.environ.get("RENDER_GIT_COMMIT")  # на Render версия приходит из окружения
    if v:
        return v
    try:
        import subprocess
        r = subprocess.run(["git", "rev-parse", "HEAD"], cwd=BASE_DIR,
                           capture_output=True, text=True, timeout=5)
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout.strip()
    except Exception:
        pass
    try:
        m = hashlib.sha256()
        for name in ("app.html",):  # только фронт: правки логики сигналов не должны разлогинивать
            p = BASE_DIR / name
            if p.exists():
                m.update(p.read_bytes())
        return m.hexdigest()
    except Exception:
        return "v1"


APP_VERSION = _code_version()

# Токен сессии не хранится в памяти, а вычисляется из логина, пароля и версии кода:
# перезапуск сервера не разлогинивает; обновление кода или смена пароля — разлогинивает всех.
SESSION_TOKEN = None if AUTH is None else hashlib.sha256(
    (AUTH["login"] + ":" + AUTH["password"] + ":" + APP_VERSION + ":rsi-monitor-salt-v1").encode()
).hexdigest()
COOKIE = "rsi_session"


def _is_authed(request: Request) -> bool:
    token = request.cookies.get(COOKIE)
    return bool(token and SESSION_TOKEN and secrets.compare_digest(token, SESSION_TOKEN))

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


# ---------------------------------------------------------------- сигналы (вебхуки)

SIGNALS_FILE = BASE_DIR / "signals.json"
SIGNALS_MAX = 300
SIGNALS = deque(maxlen=SIGNALS_MAX)

# нормализация типа сигнала к bull / bear / neutral
_BULL = {"bull", "bullish", "long", "buy", "up", "green", "лонг", "покупка", "вверх"}
_BEAR = {"bear", "bearish", "short", "sell", "down", "red", "шорт", "продажа", "вниз"}

# нормализация таймфрейма TradingView ({{interval}} шлёт "60","240","D"...) к виду 1h/4h/1d
_TF_MAP = {"1": "1m", "3": "3m", "5": "5m", "15": "15m", "30": "30m",
           "45": "45m", "60": "1h", "120": "2h", "180": "3h", "240": "4h",
           "360": "6h", "480": "8h", "720": "12h", "D": "1d", "1D": "1d",
           "W": "1w", "1W": "1w", "M": "1M"}


def _norm_base(sym: str) -> str:
    """'BINANCE:ADAUSDT.P' / 'ADAUSDT' / 'ADA' -> 'ADA'."""
    if not sym:
        return ""
    s = str(sym).upper().strip()
    if ":" in s:
        s = s.split(":", 1)[1]
    s = s.split(".", 1)[0]  # убрать .P (перпетуал)
    for q in ("USDT", "USDC", "USD", "PERP"):
        if s.endswith(q) and len(s) > len(q):
            s = s[:-len(q)]
            break
    return s


def _norm_signal(v: str) -> str:
    s = str(v or "").lower().strip()
    if s in _BULL:
        return "bull"
    if s in _BEAR:
        return "bear"
    return "neutral"


def _norm_tf(v) -> str:
    s = str(v or "").strip()
    return _TF_MAP.get(s, s)


def _load_signals():
    if SIGNALS_FILE.exists():
        try:
            for item in json.loads(SIGNALS_FILE.read_text(encoding="utf-8")):
                SIGNALS.append(item)
        except Exception as e:
            print("signals.json не прочитан:", e)


def _save_signals():
    try:
        SIGNALS_FILE.write_text(json.dumps(list(SIGNALS), ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def add_signal(payload: dict) -> dict:
    """Разобрать входящий вебхук в унифицированную запись сигнала."""
    raw_sym = payload.get("symbol") or payload.get("ticker") or ""
    price = payload.get("price")
    try:
        price = float(price) if price not in (None, "") else None
    except (TypeError, ValueError):
        price = None
    rec = {
        "ts": time.time(),
        "time": time.strftime("%H:%M:%S"),
        "date": time.strftime("%Y-%m-%d"),
        "base": _norm_base(raw_sym),
        "symbol": re.sub(r"^[A-Z]+:", "", str(raw_sym).upper()),
        "tf": _norm_tf(payload.get("tf") or payload.get("interval")),
        "indicator": str(payload.get("indicator") or payload.get("ind") or "TV")[:40],
        "signal": _norm_signal(payload.get("signal") or payload.get("side") or payload.get("action")),
        "note": str(payload.get("note") or payload.get("message") or payload.get("comment") or "")[:200],
    }
    if price is not None:
        rec["price"] = price
    SIGNALS.append(rec)
    _save_signals()
    return rec


_load_signals()


# ---------------------------------------------------------------- приложение

app = FastAPI(title="RSI Монитор")


# Пути, доступные без входа (вебхук защищён своим токеном, не cookie)
OPEN_PATHS = {"/login", "/favicon.ico", "/api/signal"}


@app.middleware("http")
async def auth_guard(request: Request, call_next):
    path = request.url.path
    if AUTH is None or path in OPEN_PATHS:
        return await call_next(request)
    if _is_authed(request):
        return await call_next(request)
    if path.startswith("/api"):
        return JSONResponse({"error": "нужен вход"}, status_code=401)
    return RedirectResponse("/login")


@app.get("/login")
def login_page(request: Request):
    if AUTH is None or _is_authed(request):
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
        resp = JSONResponse({"ok": True})
        resp.set_cookie(COOKIE, SESSION_TOKEN, max_age=30 * 24 * 3600,
                        httponly=True, samesite="lax")
        return resp
    return JSONResponse({"ok": False}, status_code=401)


@app.get("/logout")
def logout():
    resp = RedirectResponse("/login")
    resp.delete_cookie(COOKIE)
    return resp


@app.get("/")
def index():
    return FileResponse(BASE_DIR / "app.html")


@app.api_route("/api/signal", methods=["POST", "GET"])
async def api_signal(request: Request, token: str = ""):
    """Приём вебхука от Make/TradingView. Защита — токен в query (?token=...) или в теле."""
    payload = {}
    try:
        body = await request.body()
        if body:
            try:
                payload = json.loads(body)
            except Exception:
                # не-JSON тело: разберём как «KEY=VALUE; ...» или просто текст
                txt = body.decode("utf-8", "ignore")
                payload = {"note": txt}
    except Exception:
        payload = {}
    if isinstance(payload, dict):
        for k in list(payload.keys()):
            payload.setdefault(k.lower(), payload[k])
    tok = token or (payload.get("token") if isinstance(payload, dict) else "") \
          or request.query_params.get("token", "")
    if not WEBHOOK_TOKEN or not secrets.compare_digest(str(tok), str(WEBHOOK_TOKEN)):
        return JSONResponse({"ok": False, "error": "bad token"}, status_code=403)
    rec = add_signal(payload if isinstance(payload, dict) else {"note": str(payload)})
    return JSONResponse({"ok": True, "signal": rec})


@app.get("/api/signals")
def api_signals(base: str = "", limit: int = 200):
    items = list(SIGNALS)
    if base:
        b = _norm_base(base)
        items = [s for s in items if s.get("base") == b]
    items = items[-limit:][::-1]  # новые сверху
    return JSONResponse({"count": len(items), "signals": items})


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
