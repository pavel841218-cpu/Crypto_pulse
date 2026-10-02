import asyncio
import aiohttp
from aiohttp import web
import os
import time
import json
import gc
import logging
from collections import defaultdict, deque
from html import escape

# ============================================================
# QUASIMODO / CONSOL_PULSE v12.0 — Dual Pump & Fast Dump Edition
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
PORT = int(os.environ.get("PORT", "10000"))

SCAN_INTERVAL_SEC = int(os.environ.get("SCAN_INTERVAL_SEC", "40"))
OI_SAMPLE_INTERVAL_SEC = int(os.environ.get("OI_SAMPLE_INTERVAL_SEC", "300"))
MAX_UNIVERSE_SYMBOLS = int(os.environ.get("MAX_UNIVERSE_SYMBOLS", "250"))
MAX_SCAN_CANDIDATES = int(os.environ.get("MAX_SCAN_CANDIDATES", "250"))

PUMP_COOLDOWN_SEC = int(os.environ.get("PUMP_COOLDOWN_SEC", str(3 * 3600)))
DUMP_COOLDOWN_SEC = int(os.environ.get("DUMP_COOLDOWN_SEC", str(1 * 3600)))

KUCOIN_BASE = "https://api-futures.kucoin.com"
BITGET_BASE = "https://api.bitget.com"
BYBIT_BASE = "https://api.bybit.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("QUASIMODO-BOT")

SESSION = None
HTTP_SEMAPHORE = None
START_TIME = time.time()

UNIVERSE = {}
LAST_PUMP_SIGNAL = {}
LAST_DUMP_SIGNAL = {}

OI_HIST_MAXLEN = int(14 * 3600 / OI_SAMPLE_INTERVAL_SEC) + 10
OI_HISTORY = defaultdict(lambda: defaultdict(lambda: deque(maxlen=OI_HIST_MAXLEN)))

STATS = {
    "scans": 0,
    "pumps": 0,
    "dumps": 0,
}

# ============================================================
# HTTP & DATA UTILS
# ============================================================

async def http_get(url, params=None, timeout=8, retries=2):
    if SESSION is None or SESSION.closed or HTTP_SEMAPHORE is None:
        return None
    for attempt in range(retries + 1):
        try:
            async with HTTP_SEMAPHORE:
                async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                    if r.status >= 400:
                        return None
                    return await r.json(content_type=None)
        except Exception:
            if attempt < retries:
                await asyncio.sleep(0.3)
    return None

def num(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default

def norm(s):
    if not s: return ""
    s = str(s).upper()
    for suf in ("USDTM", "USDT", "-USDT", "_USDT", "PERP"):
        if s.endswith(suf):
            s = s[:-len(suf)]
            break
    return s

def _parse_list_candles(rows, ts_ms=True):
    candles = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts, o, h, l, c, v = int(row[0]), num(row[1]), num(row[2]), num(row[3]), num(row[4]), num(row[5])
            if o > 0 and c > 0:
                if ts_ms and ts < 10 ** 12: ts *= 1000
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except Exception:
            continue
    candles.sort(key=lambda x: x["ts"])
    return candles

async def fetch_kucoin_candles(symbol, granularity_min, limit):
    now_ms = int(time.time() * 1000)
    from_ms = now_ms - limit * granularity_min * 60 * 1000
    data = await http_get(f"{KUCOIN_BASE}/api/v1/kline/query", {
        "symbol": symbol, "granularity": str(granularity_min), "from": from_ms, "to": now_ms,
    })
    if not data or not isinstance(data.get("data"), list):
        return []
    return _parse_list_candles(data["data"])

# ============================================================
# АНАЛИЗ И МЕТРИКИ
# ============================================================

def calc_15m_metrics(candles_15m):
    if len(candles_15m) < 30:
        return None

    idx_now = len(candles_15m) - 1
    idx_past = idx_now - 2
    close_now = candles_15m[idx_now]["close"]
    close_past = candles_15m[idx_past]["close"]
    if close_past <= 0: return None

    pct = ((close_now / close_past) - 1) * 100.0
    base_slice = candles_15m[max(0, idx_past - 96):idx_past]
    if not base_slice: return None
    base_high = max(c["close"] for c in base_slice)

    vol_now = candles_15m[idx_now]["volume"] * candles_15m[idx_now]["close"]
    prev_vols = [c["volume"] * c["close"] for c in candles_15m[max(0, idx_now - 20):idx_now] if c["volume"] > 0]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 0.0
    rvol = vol_now / avg_vol if avg_vol > 0 else 0.0

    return {
        "close": close_now,
        "pct": pct,
        "breakout": close_now > base_high * 0.985,
        "base_high": base_high,
        "rvol": rvol,
    }

def detect_market_action(candles_15m, candles_1m, kc_metrics, oi_deltas):
    """
    Определяет статус рынка: 'DUMP' (сброс), 'PUMP' (памп) или None
    """
    if not candles_15m or not kc_metrics:
        return None, {}

    # 1. ПРОВЕРКА НА БЫСТРЫЙ СБРОС ПОЗИЦИЙ (Анализ 1m свечей за последние 3-5 мин)
    if candles_1m and len(candles_1m) >= 5:
        recent_1m = candles_1m[-5:]
        high_5m = max(c["high"] for c in recent_1m)
        close_now = recent_1m[-1]["close"]
        low_5m = min(c["low"] for c in recent_1m)
        range_5m = high_5m - low_5m

        if range_5m > 0:
            # Откат от максимума в %
            drop_from_high = ((high_5m - close_now) / range_5m) * 100.0
            
            # Анализ падения OI
            oi_vals = [v for v in oi_deltas.values() if v is not None]
            min_oi = min(oi_vals) if oi_vals else 0.0

            is_wick_dump = (drop_from_high >= 35.0) and (kc_metrics.get("rvol", 0) >= 3.5)
            is_oi_dump = (min_oi <= -2.0) and (kc_metrics.get("rvol", 0) >= 3.0)

            if is_wick_dump or is_oi_dump:
                return "DUMP", {
                    "drop_pct": drop_from_high,
                    "rvol": kc_metrics.get("rvol", 0),
                    "close": close_now,
                    "oi_deltas": oi_deltas
                }

    # 2. ПРОВЕРКА НА ВХОД ИМПУЛЬСА / ПАМПА (15m)
    if kc_metrics.get("breakout") and kc_metrics.get("pct", 0) >= 3.5:
        return "PUMP", {
            "pct": kc_metrics.get("pct"),
            "rvol": kc_metrics.get("rvol"),
            "base_high": kc_metrics.get("base_high"),
            "close": kc_metrics.get("close"),
            "oi_deltas": oi_deltas
        }

    return None, {}

# ============================================================
# TELEGRAM NOTIFICATIONS
# ============================================================

async def send_tg(text):
    if not BOT_TOKEN or not CHAT_ID or SESSION is None:
        return False
    try:
        async with SESSION.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            return r.status == 200
    except Exception:
        return False

async def send_pump_alert(base, data):
    ticker = f"<code>{escape(base)}USDT</code>"
    def _oi(val): return f"{val:+.1f}%" if val is not None else "нет данных"

    msg = (
        f"🚀 <b>ИМПУЛЬС / ПАМП: {ticker}</b>\n\n"
        f"🟢 <b>Рост:</b> +{data['pct']:.1f}%\n"
        f"💥 <b>RVOL:</b> {data['rvol']:.1f}x\n"
        f"📈 <b>Пробой базы:</b> {data['base_high']:.6g} → {data['close']:.6g}\n\n"
        f"<b>📊 Прирост OI:</b>\n"
        f"  KuCoin: {_oi(data['oi_deltas'].get('kucoin'))}\n"
        f"  Bitget: {_oi(data['oi_deltas'].get('bitget'))}\n"
        f"  Bybit:  {_oi(data['oi_deltas'].get('bybit'))}\n\n"
        f"🎯 <b>Статус:</b> Вход / Набор позиции"
    )
    sent = await send_tg(msg)
    if sent: STATS["pumps"] += 1
    return sent

async def send_dump_alert(base, data):
    ticker = f"<code>{escape(base)}USDT</code>"
    def _oi(val): return f"{val:+.1f}%" if val is not None else "нет данных"

    msg = (
        f"🔴 <b>НАЧАЛСЯ СБРОС ПОЗИЦИЙ: {ticker}</b>\n\n"
        f"⚠️ Слив об маркет на 1m/5m таймфрейме!\n\n"
        f"📉 <b>Откат от пика:</b> -{data['drop_pct']:.1f}%\n"
        f"🔥 <b>RVOL:</b> {data['rvol']:.1f}x\n"
        f"💰 <b>Текущая цена:</b> {data['close']:.6g}\n\n"
        f"<b>📊 Отток OI (Фиксация):</b>\n"
        f"  KuCoin: {_oi(data['oi_deltas'].get('kucoin'))}\n"
        f"  Bitget: {_oi(data['oi_deltas'].get('bitget'))}\n"
        f"  Bybit:  {_oi(data['oi_deltas'].get('bybit'))}\n\n"
        f"🛑 <b>Статус:</b> Выход / Фиксация прибыли"
    )
    sent = await send_tg(msg)
    if sent: STATS["dumps"] += 1
    return sent

# ============================================================
# PROCESS SYMBOL
# ============================================================

async def process_symbol(base):
    item = UNIVERSE.get(base)
    if not item: return
    now = time.time()

    # Параллельно тянем 15m свечи (памп) и 1m свечи (быстрый сброс)
    candles_15m = await fetch_kucoin_candles(item["kucoin_symbol"], 15, 60)
    candles_1m = await fetch_kucoin_candles(item["kucoin_symbol"], 1, 15)
    
    kc = calc_15m_metrics(candles_15m)
    oi_deltas = {"kucoin": 0.0, "bitget": 0.0, "bybit": 0.0} # Заглушка/Интеграция с вашей функцией OI

    action, data = detect_market_action(candles_15m, candles_1m, kc, oi_deltas)

    # Приоритет №1: Сигнал о СБРОСЕ
    if action == "DUMP" and (now - LAST_DUMP_SIGNAL.get(base, 0) >= DUMP_COOLDOWN_SEC):
        LAST_DUMP_SIGNAL[base] = now
        await send_dump_alert(base, data)
        log.info("🔴 DUMP ALERT: %s | Откат: %.1f%%", base, data["drop_pct"])
        return

    # Приоритет №2: Сигнал о ПАМПЕ
    if action == "PUMP" and (now - LAST_PUMP_SIGNAL.get(base, 0) >= PUMP_COOLDOWN_SEC):
        LAST_PUMP_SIGNAL[base] = now
        await send_pump_alert(base, data)
        log.info("🚀 PUMP ALERT: %s | Рост: %.1f%%", base, data["pct"])

# ============================================================
# MAIN SCAN LOOP
# ============================================================

async def fetch_kucoin_contracts():
    data = await http_get(f"{KUCOIN_BASE}/api/v1/contracts/active")
    res = {}
    if data and isinstance(data.get("data"), list):
        for r in data["data"]:
            if str(r.get("status","")).lower() == "open" and str(r.get("settleCurrency","")).upper() == "USDT":
                base = norm(r.get("baseCurrency") or r.get("symbol"))
                if base:
                    res[base] = {"kucoin_symbol": r.get("symbol"), "volume24": num(r.get("turnoverOf24h")), "price": num(r.get("lastTradePrice"))}
    return res

async def refresh_universe():
    global UNIVERSE
    kc = await fetch_kucoin_contracts()
    if not kc: return
    sorted_u = sorted(kc.items(), key=lambda x: x[1]["volume24"], reverse=True)
    UNIVERSE = dict(sorted_u[:MAX_UNIVERSE_SYMBOLS])
    log.info("🌐 Юниверс обновлен: %d пар", len(UNIVERSE))

async def scan_loop():
    while True:
        try:
            if UNIVERSE:
                STATS["scans"] += 1
                sem = asyncio.Semaphore(12)
                async def worker(b):
                    async with sem:
                        await process_symbol(b)
                await asyncio.gather(*[worker(b) for b in list(UNIVERSE.keys())[:MAX_SCAN_CANDIDATES]])
                gc.collect()
        except Exception:
            pass
        await asyncio.sleep(SCAN_INTERVAL_SEC)

async def start(app):
    global SESSION, HTTP_SEMAPHORE, START_TIME
    START_TIME = time.time()
    if SESSION and not SESSION.closed: await SESSION.close()
    SESSION = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=30, ttl_dns_cache=300))
    HTTP_SEMAPHORE = asyncio.Semaphore(12)
    await refresh_universe()
    app["scan_task"] = asyncio.create_task(scan_loop())

async def stop(app):
    if app.get("scan_task"): app["scan_task"].cancel()
    global SESSION
    if SESSION and not SESSION.closed:
        await SESSION.close()
        await asyncio.sleep(0.250)

app = web.Application()
app.router.add_get("/", lambda r: web.Response(text="QUASIMODO BOT ONLINE"))
app.on_startup.append(start)
app.on_cleanup.append(stop)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
