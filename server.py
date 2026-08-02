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
import hashlib
import json
import os
import secrets
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
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

# RSI 12h нет в scanner — считаем по свечам Binance, кэшируем отдельно (меняется медленно)
_rsi12_cache = {"ts": 0.0, "map": {}}
RSI12_TTL = 300


def _binance_klines(symbol: str, interval: str, limit: int):
    url = ("https://api.binance.com/api/v3/klines"
           f"?symbol={symbol}&interval={interval}&limit={limit}")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


def _rsi(closes, period: int = 14):
    """RSI по Уайлдеру (как в TradingView)."""
    if len(closes) < period + 1:
        return None
    gains = losses = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        if d >= 0:
            gains += d
        else:
            losses -= d
    avg_g, avg_l = gains / period, losses / period
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        g = d if d > 0 else 0.0
        l = -d if d < 0 else 0.0
        avg_g = (avg_g * (period - 1) + g) / period
        avg_l = (avg_l * (period - 1) + l) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100.0 - 100.0 / (1.0 + rs)


def _atr_pct(kl, period: int = 14):
    """ATR(14) по Уайлдеру в процентах от текущей цены. kl — свечи Binance."""
    if len(kl) < period + 1:
        return None
    highs = [float(k[2]) for k in kl]
    lows = [float(k[3]) for k in kl]
    closes = [float(k[4]) for k in kl]
    trs = []
    for i in range(1, len(kl)):
        trs.append(max(highs[i] - lows[i],
                       abs(highs[i] - closes[i - 1]),
                       abs(lows[i] - closes[i - 1])))
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
    price = closes[-1]
    if price == 0:
        return None
    return round(atr / price * 100, 1)


# ---- доп. индикаторы из свечей (EMA-тренд, Chandelier Exit, дивергенция, ATR-дно) ----
# состояние: 1 = бык/дно, -1 = медведь, 0 = нейтрально, None = мало данных

def _ema_last(values, period):
    if len(values) < period:
        return None
    k = 2.0 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e


def _ema_trend(closes):
    """EMA(9) против EMA(21): 1 бык / -1 медведь / 0 плоско."""
    if len(closes) < 22 or closes[-1] == 0:
        return None
    e9, e21 = _ema_last(closes, 9), _ema_last(closes, 21)
    if e9 is None or e21 is None:
        return None
    d = (e9 - e21) / closes[-1]
    if abs(d) < 0.0015:
        return 0
    return 1 if d > 0 else -1


def _atr_series(kl, period=14):
    """ATR по Уайлдеру — весь ряд (индексы совпадают с kl)."""
    if len(kl) < period + 1:
        return None
    highs = [float(k[2]) for k in kl]
    lows = [float(k[3]) for k in kl]
    closes = [float(k[4]) for k in kl]
    trs = [max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
           for i in range(1, len(kl))]
    out = [None] * len(kl)
    atr = sum(trs[:period]) / period
    out[period] = atr
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
        out[i + 1] = atr
    return out


def _chandelier(kl, period=22, mult=3.0):
    """Chandelier Exit — направление тренда: 1 long / -1 short."""
    if len(kl) < period + 2:
        return None
    highs = [float(k[2]) for k in kl]
    lows = [float(k[3]) for k in kl]
    closes = [float(k[4]) for k in kl]
    atrs = _atr_series(kl, period if period <= 14 else 14)
    if atrs is None:
        return None
    prev_long = prev_short = None
    direction = 1
    for i in range(period, len(kl)):
        a = atrs[i]
        if a is None:
            continue
        hh = max(highs[i - period + 1:i + 1])
        ll = min(lows[i - period + 1:i + 1])
        long_stop = hh - mult * a
        short_stop = ll + mult * a
        if prev_long is not None and closes[i - 1] > prev_long:
            long_stop = max(long_stop, prev_long)
        if prev_short is not None and closes[i - 1] < prev_short:
            short_stop = min(short_stop, prev_short)
        if prev_short is not None and closes[i] > prev_short:
            direction = 1
        elif prev_long is not None and closes[i] < prev_long:
            direction = -1
        prev_long, prev_short = long_stop, short_stop
    return direction


def _rsi_series(closes, period=14):
    """RSI по Уайлдеру для каждого бара (для дивергенции)."""
    if len(closes) < period + 1:
        return None
    out = [None] * len(closes)
    gains = losses = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        if d >= 0:
            gains += d
        else:
            losses -= d
    ag, al = gains / period, losses / period
    out[period] = 100.0 if al == 0 else 100.0 - 100.0 / (1 + ag / al)
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        g = d if d > 0 else 0.0
        l = -d if d < 0 else 0.0
        ag = (ag * (period - 1) + g) / period
        al = (al * (period - 1) + l) / period
        out[i] = 100.0 if al == 0 else 100.0 - 100.0 / (1 + ag / al)
    return out


def _pivot_lows(vals, left=3, right=3):
    """индексы свинг-минимумов."""
    idx = []
    for i in range(left, len(vals) - right):
        w = vals[i - left:i + right + 1]
        if any(x is None for x in w):
            continue
        if vals[i] == min(w) and w.count(vals[i]) == 1:
            idx.append(i)
    return idx


def _divergence(closes, rsis):
    """Приближение: последние два свинг-лоу/хай цены против RSI. 1 бычья / -1 медвежья / 0."""
    if rsis is None or len(closes) < 40:
        return 0
    lows = _pivot_lows(closes)
    highs = _pivot_lows([-c for c in closes])
    if len(lows) >= 2:
        a, b = lows[-2], lows[-1]
        if b - a >= 3 and closes[b] < closes[a] and rsis[a] is not None and rsis[b] is not None and rsis[b] > rsis[a]:
            return 1
    if len(highs) >= 2:
        a, b = highs[-2], highs[-1]
        if b - a >= 3 and closes[b] > closes[a] and rsis[a] is not None and rsis[b] is not None and rsis[b] < rsis[a]:
            return -1
    return 0


def _atr_bottom(kl, atrs, look=20, recent=6, k=1.2):
    """Приближение 'Дно по ATR': СВЕЖИЙ минимум (в последних `recent` барах) + отскок на k·ATR. 1 дно / 0 нет."""
    if atrs is None or len(kl) < look + 2:
        return 0
    lows = [float(x[3]) for x in kl]
    closes = [float(x[4]) for x in kl]
    a = atrs[-1]
    if a is None or a == 0:
        return 0
    seg = lows[-look:]
    recent_low = min(seg)
    low_idx = len(lows) - look + seg.index(recent_low)
    # дно = минимум последних `look` баров пришёлся на последние `recent` баров, но не на текущий, и цена отскочила на k·ATR
    if len(lows) - recent <= low_idx <= len(lows) - 2 and (closes[-1] - recent_low) >= k * a:
        return 1
    return 0


def _klines_inds(kl):
    """Пакет доп. индикаторов из свечей: EMA-тренд / CE / дивергенция / ATR-дно."""
    try:
        closes = [float(k[4]) for k in kl]
        atrs = _atr_series(kl)
        rsis = _rsi_series(closes)
        return {"ema": _ema_trend(closes), "ce": _chandelier(kl),
                "div": _divergence(closes, rsis), "atrb": _atr_bottom(kl, atrs)}
    except Exception:
        return {"ema": None, "ce": None, "div": 0, "atrb": 0}


def kl12h_for(bases) -> dict:
    """По 12h-свечам Binance: RSI 12h, изменение за 12ч, ATR% 12h. base -> {...}."""
    now = time.time()
    if _rsi12_cache["map"] and now - _rsi12_cache["ts"] < RSI12_TTL:
        return _rsi12_cache["map"]
    out = {}

    def work(base):
        try:
            kl = _binance_klines(base + "USDT", "12h", 200)
            closes = [float(k[4]) for k in kl]
            info = {}
            v = _rsi(closes)
            if v is not None:
                info["rsi12h"] = round(v, 1)
            if len(closes) >= 2 and closes[-2] != 0:
                info["change12h"] = round((closes[-1] / closes[-2] - 1) * 100, 2)
            a = _atr_pct(kl)
            if a is not None:
                info["atr12"] = a
            info.update(_klines_inds(kl))   # ema/ce/div/atrb для 12h
            if info:
                out[base] = info
        except Exception:
            pass

    try:
        with ThreadPoolExecutor(max_workers=16) as ex:
            list(ex.map(work, bases))
    except Exception:
        pass
    if out:
        _rsi12_cache["map"] = out
        _rsi12_cache["ts"] = now
    return _rsi12_cache["map"]


_atr_cache = {}   # interval -> {"ts":..., "map": {base: atr%}}
ATR_TTL = 300


def atr_for(bases, interval) -> dict:
    """ATR% по свечам Binance для заданного интервала (1h/4h). base -> atr%."""
    c = _atr_cache.setdefault(interval, {"ts": 0.0, "map": {}})
    now = time.time()
    if c["map"] and now - c["ts"] < ATR_TTL:
        return c["map"]
    out = {}

    def work(base):
        try:
            kl = _binance_klines(base + "USDT", interval, 100)
            rec = _klines_inds(kl)      # ema/ce/div/atrb
            rec["atr"] = _atr_pct(kl)
            out[base] = rec
        except Exception:
            pass

    try:
        with ThreadPoolExecutor(max_workers=16) as ex:
            list(ex.map(work, bases))
    except Exception:
        pass
    if out:
        c["map"] = out
        c["ts"] = now
    return c["map"]


def _rating_label(rec):
    """Recommend.All (-1..1 из TradingView) -> текст."""
    if rec is None:
        return None
    if rec >= 0.1:
        return "Buy"
    if rec <= -0.1:
        return "Sell"
    return "Neutral"


# ---------------------------------------------------------------- уведомления (Telegram)

def _read_auth_file() -> dict:
    f = BASE_DIR / "auth.json"
    if f.exists():
        try:
            return json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


_af = _read_auth_file()
TG_TOKEN = os.environ.get("TG_TOKEN") or _af.get("tg_token")
TG_CHAT = os.environ.get("TG_CHAT") or _af.get("tg_chat")
FIRE_THRESHOLD_SRV = float(os.environ.get("FIRE_THRESHOLD") or _af.get("fire_threshold") or 10)
LOOP_SEC = 60          # как часто фоновый детектор проверяет рынок
ALERTS_FILE = BASE_DIR / "alerts.json"

_srv_prev_prob = {}    # серверная база отсчёта вероятности (для детекта огонька)


def telegram_send(text: str, buttons=None) -> bool:
    """Отправить сообщение в Telegram. buttons — inline-клавиатура вида [[{text,url}]]."""
    if not TG_TOKEN or not TG_CHAT:
        print("[TG] нет токена/chat — сообщение не отправлено:", text[:80])
        return False
    try:
        params = {
            "chat_id": str(TG_CHAT), "text": text, "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }
        if buttons:
            params["reply_markup"] = json.dumps({"inline_keyboard": buttons})
        data = urllib.parse.urlencode(params).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            data=data, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read()).get("ok", False)
    except Exception as e:
        print("[TG] ошибка отправки:", e)
        return False


def _prob(rsi):
    """Та же формула вероятности пробоя, что на фронте. -> (prob:int, kind:str)."""
    if rsi is None:
        return 0, ""
    lo, hi, zone = 30, 70, 5
    if rsi <= lo:
        return min(99, round(80 + (lo - rsi) * 3)), "dn"
    if rsi >= hi:
        return min(99, round(80 + (rsi - hi) * 3)), "up"
    d_lo, d_hi = rsi - lo, hi - rsi
    d = min(d_lo, d_hi)
    if d <= zone:
        return round(50 + (zone - d) * 6), ("dn" if d_lo < d_hi else "up")
    return max(4, round(40 - 2 * d)), ""


def load_alerts() -> dict:
    if ALERTS_FILE.exists():
        try:
            return json.loads(ALERTS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_alerts(a: dict):
    try:
        ALERTS_FILE.write_text(json.dumps(a, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


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
        for name in ("server.py", "app.html"):
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
    cols = ["name", "close", "change|60", "change|240", "change", "Perf.W",
            "RSI|60", "RSI|240", "volume", "Volatility.D", "Recommend.All"]
    tickers = ["BINANCE:" + b + "USDT" for b in bases]
    pair_raw = _tv_request(PAIR_SCAN_URL, {
        "columns": cols,
        "symbols": {"query": {"types": []}, "tickers": tickers},
    })
    by_name = {}
    for row in pair_raw.get("data", []):
        d = row["d"]
        if d and d[0]:
            by_name[d[0]] = d

    coins, chosen_bases = [], []
    for base in bases:
        d = by_name.get(base + "USDT")
        if not d:
            continue
        _name, close, chg1h, chg4h, chg24h, chg7d, rsi1h, rsi4h, vol, volat, rec = d
        if close is None or rsi4h is None:
            continue
        coins.append({
            "sym": base + "USDT",
            "base": base,
            "price": close,
            "change1h": round(chg1h, 2) if chg1h is not None else None,
            "change4h": round(chg4h, 2) if chg4h is not None else None,
            "change24h": round(chg24h, 2) if chg24h is not None else None,
            "change7d": round(chg7d, 2) if chg7d is not None else None,
            "rsi1h": round(rsi1h, 1) if rsi1h is not None else None,
            "rsi4h": round(rsi4h, 1),
            "volUsd": (vol * close) if (vol is not None and close is not None) else None,
            "volat": round(volat, 2) if volat is not None else None,
            "rating": _rating_label(rec),
            "mcap": mcap_by_base.get(base),
        })
        chosen_bases.append(base)
        if len(coins) >= TOP_N:
            break

    # RSI 12h, Δ12ч, ATR% 12h — по 12h-свечам Binance; ATR% 1h/4h — отдельные свечи
    kl12 = kl12h_for(chosen_bases)
    atr1 = atr_for(chosen_bases, "1h")
    atr4 = atr_for(chosen_bases, "4h")
    for c in coins:
        info = kl12.get(c["base"], {})
        c["rsi12h"] = info.get("rsi12h")
        c["change12h"] = info.get("change12h")
        c["atr12"] = info.get("atr12")
        c["ema12"] = info.get("ema")
        c["ce12"] = info.get("ce")
        c["div12"] = info.get("div")
        c["atrb12"] = info.get("atrb")
        d1 = atr1.get(c["base"]) or {}
        d4 = atr4.get(c["base"]) or {}
        c["atr1"] = d1.get("atr")
        c["atr4"] = d4.get("atr")
        c["ema1"] = d1.get("ema");  c["ema4"] = d4.get("ema")
        c["ce1"] = d1.get("ce");    c["ce4"] = d4.get("ce")
        c["div1"] = d1.get("div");  c["div4"] = d4.get("div")
        c["atrb1"] = d1.get("atrb"); c["atrb4"] = d4.get("atrb")

    return {
        "updated": time.strftime("%H:%M:%S"),
        "source": "TradingView (scanner)",
        "count": len(coins),
        "coins": coins,
    }


# ---------------------------------------------------------------- фоновый детектор

def _detect_and_notify():
    """Один проход: тянем рынок, ищем огоньки (и позже — ценовые алерты), шлём в Telegram."""
    try:
        data = fetch_top100()
    except Exception as e:
        print("[loop] данные не получены:", e)
        return
    coins = data.get("coins", [])
    fired = []
    for c in coins:
        base = c["base"]
        p, kind = _prob(c.get("rsi4h"))
        prev = _srv_prev_prob.get(base)
        if prev is not None and p - prev >= FIRE_THRESHOLD_SRV:
            arrow = "↓" if kind == "dn" else "↑" if kind == "up" else "•"
            fired.append(f"🔥 <b>{base}</b>  {prev}% → {p}% (+{p - prev}) {arrow}  RSI4h={c.get('rsi4h')}")
        _srv_prev_prob[base] = p
    if fired:
        telegram_send("🔥 <b>Огоньки</b>\n" + "\n".join(fired[:20]))


def _loop():
    print(f"[loop] фоновый детектор запущен, интервал {LOOP_SEC}с, порог {FIRE_THRESHOLD_SRV}%")
    while True:
        _detect_and_notify()
        time.sleep(LOOP_SEC)


def start_background():
    if TG_TOKEN and TG_CHAT:
        threading.Thread(target=_loop, daemon=True).start()
    else:
        print("[loop] Telegram не настроен (tg_token/tg_chat) — фоновый детектор не запущен")


# ---------------------------------------------------------------- приложение

app = FastAPI(title="RSI Монитор")


@app.on_event("startup")
def _on_startup():
    # уведомления шлёт браузер при обновлении сайта; серверный цикл — только по флагу SERVER_LOOP=1
    if os.environ.get("SERVER_LOOP") == "1":
        start_background()
    else:
        print("[loop] серверный детектор выключен — уведомления шлёт браузер по обновлению сайта")


@app.middleware("http")
async def auth_guard(request: Request, call_next):
    path = request.url.path
    if AUTH is None or path == "/login" or path == "/favicon.ico":
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


def _norm_base(sym: str) -> str:
    """'BINANCE:ADAUSDT' / 'ADAUSDT' / 'ADA' -> 'ADA'."""
    if not sym:
        return ""
    s = str(sym).upper().strip()
    if ":" in s:
        s = s.split(":", 1)[1]
    for q in ("USDT", "USDC", "USD"):
        if s.endswith(q) and len(s) > len(q):
            return s[:-len(q)]
    return s


@app.get("/api/analyze")
def api_analyze(coin: str = ""):
    """ИИ-анализ выбранной монеты.

    Пока заглушка: собираем контекст и возвращаем демо-текст. Позже здесь будет
    вызов Claude API с промт-шаблоном + данные монеты и её история (свечи Binance).
    Ключ хранить в env ANTHROPIC_API_KEY (не в коде).
    """
    base = _norm_base(coin) if coin else ""
    ctx = None
    if _cache["data"]:
        ctx = next((c for c in _cache["data"]["coins"] if c["base"] == base), None)

    if not base:
        return JSONResponse({"ready": False, "text": "Монета не выбрана."})

    if ctx:
        lines = [
            f"Монета: {ctx['base']}/USDT",
            f"Цена: {ctx.get('price')}$   Δ24ч: {ctx.get('change24h')}%   Δ7д: {ctx.get('change7d')}%",
            f"RSI 1ч/4ч/12ч: {ctx.get('rsi1h')} / {ctx.get('rsi4h')} / {ctx.get('rsi12h')}",
            f"Волатильность: {ctx.get('volat')}%   Рейтинг TV: {ctx.get('rating')}",
        ]
        summary = "\n".join(lines)
    else:
        summary = f"Монета {base}: данные ещё не загружены (обновите таблицу)."

    text = ("Демо-режим ИИ-анализа.\n\nСобранный контекст, который уйдёт модели:\n\n"
            + summary +
            "\n\nПосле подключения ключа Anthropic здесь появится разбор: тренд по таймфреймам, "
            "близость к пробою, сила сигнала и рекомендация с обоснованием.")
    return JSONResponse({"ready": False, "coin": base, "text": text})


@app.get("/api/tg-test")
def api_tg_test():
    """Отправить тестовое сообщение в Telegram (проверка настройки)."""
    if not TG_TOKEN or not TG_CHAT:
        return JSONResponse({"ok": False, "error": "Telegram не настроен (tg_token/tg_chat в auth.json)"})
    ok = telegram_send("✅ RSI Монитор: тестовое уведомление. Связь работает.")
    return JSONResponse({"ok": ok})


# ссылка кнопки «Купить» — на страницу монеты на бирже (без ключей, покупка вручную)
BUY_URL_TEMPLATE = os.environ.get("BUY_URL") or _af.get("buy_url") or "https://www.binance.com/en/trade/{base}_USDT"


@app.post("/api/notify")
async def api_notify(request: Request):
    """Одно уведомление на один огонёк (браузер зовёт по каждому новому срабатыванию)."""
    try:
        d = await request.json()
    except Exception:
        d = {}
    base = str(d.get("base", "?")).upper()
    frm, to, delta = d.get("from"), d.get("to"), d.get("delta")
    rsi = d.get("rsi4h")
    kind = d.get("kind", "")
    tf = d.get("tf", "4h")          # таймфрейм индикатора (основа значения)
    win = d.get("win") or "—"       # окно срабатывания = интервал обновления сайта
    # направление пробоя: к 70 = UP, к 30 = DOWN
    dir_tag = "📈 UP" if kind == "up" else "📉 DOWN" if kind == "dn" else "•"

    url = BUY_URL_TEMPLATE.replace("{base}", base)
    signal = to is not None and to >= 90
    if signal and kind == "dn":
        # пробой ВНИЗ (перепродан) → покупка
        text = ("🟢 <b>BUY</b>\n"
                f"🔥 {dir_tag}\n"
                f"🪙 {base}/USDT\n"
                f"⏱ {win}\n"
                f"📊 {frm}% → <b>{to}%</b> (+{delta})\n"
                f"〽️ RSI {tf}: {rsi}")
        buttons = [[{"text": f"🛒 Купить {base}", "url": url}]]
    elif signal and kind == "up":
        # пробой ВВЕРХ (перекуплен) → продажа
        text = ("🔴 <b>SELL</b>\n"
                f"🔥 {dir_tag}\n"
                f"🪙 {base}/USDT\n"
                f"⏱ {win}\n"
                f"📊 {frm}% → <b>{to}%</b> (+{delta})\n"
                f"〽️ RSI {tf}: {rsi}")
        buttons = [[{"text": f"🔻 Продать {base}", "url": url}]]
    else:
        # анализ (нет чёткого направления или <90): без кнопки
        text = ("🔍 <b>ANALYSIS</b>\n"
                f"🔥 {dir_tag}\n"
                f"🪙 {base}/USDT\n"
                f"⏱ {win}\n"
                f"📊 {frm}% → {to}% (+{delta})\n"
                f"〽️ RSI {tf}: {rsi}")
        buttons = None
    ok = telegram_send(text, buttons=buttons)
    return JSONResponse({"ok": ok})


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
