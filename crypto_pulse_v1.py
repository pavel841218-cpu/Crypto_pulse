import asyncio
import aiohttp
from aiohttp import web
import os
import time
import logging
from collections import defaultdict, deque

# ============================================================
# PAМП-ХАНТЕР v9.0 — Multi-Exchange Pump Detection
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
PORT = int(os.environ.get("PORT", "10000"))

SCAN_INTERVAL_SEC = 15
UNIVERSE_REFRESH_SEC = 300

MAX_UNIVERSE_SYMBOLS = 200
MAX_SCAN_CANDIDATES = 120

MIN_24H_VOLUME_USDT = 300_000
MIN_PRICE_USDT = 0.0001
MAX_PRICE_USDT = 10.0

# === Триггер пампа (KuCoin 1m) ===
TRIGGER_RVOL_1M = 3.0
TRIGGER_MOVE_1M = 1.0
TRIGGER_MIN_VOLUME_USD = 30_000
TRIGGER_MAX_MOVE_1M = 25.0

# === Подтверждение ===
CONFIRM_RVOL = 2.0
CONFIRM_MOVE = 0.5
MIN_CONFIRMATIONS = 2

# === OI ===
OI_MIN_GROWTH_PCT = 2.0
OI_MIN_SOURCES = 2

# === Защита ===
MAX_24H_CHANGE = 25.0
COOLDOWN_SEC = 2 * 3600
MIN_CANDLES = 20

KUCOIN_BASE = "https://api-futures.kucoin.com"
BITGET_BASE = "https://api.bitget.com"
BYBIT_BASE = "https://api.bybit.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("PUMP-HUNTER-v9")

SESSION = None
HTTP_SEMAPHORE = None

UNIVERSE = {}
LAST_SIGNAL = {}
OI_HISTORY = defaultdict(lambda: deque(maxlen=30))

STATS = {
    "scans": 0,
    "pump_triggers": 0,
    "signals": 0,
    "rejected_no_bitget": 0,
    "rejected_no_bybit": 0,
    "rejected_no_confirm": 0,
    "rejected_24h": 0,
    "rejected_no_oi": 0,
}

# ============================================================
# HTTP
# ============================================================

async def http_get(url, params=None, timeout=6, retries=2):
    if SESSION is None or SESSION.closed or HTTP_SEMAPHORE is None:
        return None

    for attempt in range(retries + 1):
        try:
            async with HTTP_SEMAPHORE:
                async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                    if r.status == 429:
                        if attempt < retries:
                            await asyncio.sleep(1.0 * (attempt + 1))
                            continue
                        return None
                    if r.status >= 400:
                        return None
                    return await r.json(content_type=None)
        except (asyncio.TimeoutError, aiohttp.ClientError):
            if attempt < retries:
                await asyncio.sleep(0.4)
        except Exception:
            break
    return None


def num(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def norm(s):
    if not s:
        return ""
    s = str(s).upper()
    if s.startswith("XBT"):
        s = "BTC" + s[3:]
    for suf in ("USDTM", "USDT", "-USDT", "_USDT", "PERP"):
        if s.endswith(suf):
            s = s[:-len(suf)]
            break
    return s


# ============================================================
# FETCHERS
# ============================================================

async def fetch_kucoin_contracts():
    data = await http_get(f"{KUCOIN_BASE}/api/v1/contracts/active")
    result = {}
    if not data or not isinstance(data.get("data"), list):
        return result
    for row in data["data"]:
        if not isinstance(row, dict):
            continue
        if str(row.get("status", "")).lower() != "open":
            continue
        if str(row.get("settleCurrency", "")).upper() != "USDT":
            continue
        symbol = str(row.get("symbol", "")).upper()
        base = norm(row.get("baseCurrency") or symbol)
        if not base:
            continue
        result[base] = {
            "symbol": symbol,
            "price": num(row.get("lastTradePrice") or row.get("markPrice")),
            "volume24": num(row.get("turnoverOf24h")),
            "change24": num(row.get("priceChgPct")) * 100,
        }
    return result


async def fetch_kucoin_candles_1m(symbol):
    data = await http_get(f"{KUCOIN_BASE}/api/v1/kline/query", {"symbol": symbol, "granularity": "1"})
    return _parse_kucoin_candles(data)


async def fetch_kucoin_candles_5m(symbol):
    data = await http_get(f"{KUCOIN_BASE}/api/v1/kline/query", {"symbol": symbol, "granularity": "5"})
    return _parse_kucoin_candles(data)


def _parse_kucoin_candles(data):
    if not data or not isinstance(data.get("data"), list):
        return []
    candles = []
    for row in data["data"]:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts, o, h, l, c, v = int(row[0]), num(row[1]), num(row[2]), num(row[3]), num(row[4]), num(row[5])
            if o > 0 and c > 0:
                candles.append({"ts": ts * 1000, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except (TypeError, ValueError, IndexError):
            continue
    candles.sort(key=lambda x: x["ts"])
    return candles


async def fetch_kucoin_oi(symbol):
    if not symbol:
        return 0.0
    data = await http_get(f"{KUCOIN_BASE}/api/v1/contracts/{symbol}")
    if data and isinstance(data.get("data"), dict):
        return num(data["data"].get("openInterest"))
    return 0.0


async def fetch_bitget_tickers():
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/tickers", {"productType": "USDT-FUTURES"})
    result = {}
    if not data or data.get("code") != "00000":
        return result
    for row in data.get("data", []):
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).upper()
        if symbol.endswith("USDT"):
            base = norm(symbol)
            if base:
                result[base] = {"symbol": symbol}
    return result


async def fetch_bitget_candles_1m(symbol):
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/candles", {
        "symbol": symbol, "productType": "USDT-FUTURES",
        "granularity": "1m", "limit": "60",
    })
    return _parse_bitget_candles(data)


def _parse_bitget_candles(data):
    if not data or data.get("code") != "00000":
        return []
    candles = []
    for row in data.get("data", []):
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts, o, h, l, c, v = int(row[0]), num(row[1]), num(row[2]), num(row[3]), num(row[4]), num(row[5])
            if o > 0 and c > 0:
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except (TypeError, ValueError, IndexError):
            continue
    candles.sort(key=lambda x: x["ts"])
    return candles


async def fetch_bitget_oi(symbol):
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/open-interest", {
        "symbol": symbol, "productType": "USDT-FUTURES",
    })
    if not data or data.get("code") != "00000":
        return 0.0
    raw = data.get("data")
    row = {}
    if isinstance(raw, dict):
        items = raw.get("list")
        row = items[0] if isinstance(items, list) and items else raw
    elif isinstance(raw, list) and raw:
        row = raw[0]
    if not isinstance(row, dict):
        return 0.0
    return num(row.get("amount") or row.get("openInterest") or row.get("openInterestUsd"))


async def fetch_bybit_candles_1m(base):
    data = await http_get(f"{BYBIT_BASE}/v5/market/kline", {
        "category": "linear", "symbol": f"{base}USDT", "interval": "1", "limit": 60,
    })
    if not data or data.get("retCode") != 0:
        return []
    result = data.get("result")
    if not isinstance(result, dict):
        return []
    items = result.get("list")
    if not isinstance(items, list):
        return []
    candles = []
    for row in items:
        try:
            ts, o, h, l, c, v = int(row[0]), num(row[1]), num(row[2]), num(row[3]), num(row[4]), num(row[5])
            if o > 0 and c > 0:
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except (TypeError, ValueError, IndexError):
            continue
    candles.sort(key=lambda x: x["ts"])
    return candles


async def fetch_bybit_oi(base):
    data = await http_get(f"{BYBIT_BASE}/v5/market/open-interest", {
        "category": "linear", "symbol": f"{base}USDT", "intervalTime": "1min", "limit": 1,
    })
    if not data or data.get("retCode") != 0:
        return 0.0
    result = data.get("result")
    if not isinstance(result, dict):
        return 0.0
    items = result.get("list")
    if not isinstance(items, list) or not items:
        return 0.0
    return num(items[0].get("openInterest"))


# ============================================================
# METRICS
# ============================================================

def calc_pump_metrics(candles):
    """Метрики на закрытой 1m свече."""
    if len(candles) < MIN_CANDLES:
        return None
    c = candles[-2]
    prev = candles[:-2]
    if c["open"] <= 0:
        return None

    vol_usd = c["volume"] * c["close"]
    move = ((c["close"] / c["open"]) - 1) * 100

    vols = [(x["volume"] * x["close"]) for x in prev[-15:] if x["volume"] > 0]
    if not vols:
        return None
    avg = sum(vols) / len(vols)
    rvol = vol_usd / avg if avg > 0 else 0

    return {
        "close": c["close"],
        "move_pct": move,
        "rvol": rvol,
        "volume_usd": vol_usd,
    }


# ============================================================
# TELEGRAM
# ============================================================

async def send_tg(text):
    if not BOT_TOKEN or not CHAT_ID or SESSION is None:
        return False
    try:
        async with SESSION.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": CHAT_ID, "text": text,
                "parse_mode": "HTML", "disable_web_page_preview": True,
            },
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            if r.status != 200:
                return False
            payload = await r.json(content_type=None)
            return payload.get("ok", False)
    except Exception:
        return False


async def send_pump_signal(base, kc, bg, bb, oi_deltas, change_24h):
    from html import escape
    kc_d = oi_deltas.get("kucoin", 0)
    bg_d = oi_deltas.get("bitget", 0)
    bb_d = oi_deltas.get("bybit", 0)

    conf_lines = []
    if bg:
        conf_lines.append(f"  Bitget: +{bg['move_pct']:.2f}% | RVOL {bg['rvol']:.1f}x")
    if bb:
        conf_lines.append(f"  Bybit:  +{bb['move_pct']:.2f}% | RVOL {bb['rvol']:.1f}x")

    msg = (
        f"🚀🚀🚀 <b>ПАМП: {escape(base)}USDT</b>\n\n"
        f"<b>🎯 KuCoin (триггер)</b>\n"
        f"  Move: <b>+{kc['move_pct']:.2f}%</b>\n"
        f"  RVOL: <b>{kc['rvol']:.2f}x</b>\n"
        f"  Объём: ${kc['volume_usd']:,.0f}\n\n"
        f"<b>✅ Подтверждение</b>\n" + "\n".join(conf_lines) + "\n\n"
        f"<b>📈 OI Δ (5 мин)</b>\n"
        f"  KuCoin: {kc_d:+.2f}%\n"
        f"  Bitget: {bg_d:+.2f}%\n"
        f"  Bybit:  {bb_d:+.2f}%\n\n"
        f"<b>📊 24h change:</b> {change_24h:+.2f}%\n\n"
        f"⚡ Три биржи синхронно. Реальный памп!\n"
        f"🔗 <a href='https://www.coinglass.com/tv/ru/BingX_{escape(base)}USDT'>Coinglass</a>"
    )
    sent = await send_tg(msg)
    if sent:
        STATS["signals"] += 1
    return sent


# ============================================================
# CORE
# ============================================================

async def process_symbol(base):
    item = UNIVERSE.get(base)
    if not item:
        return

    # === 1. Проверка cooldown ===
    if time.time() - LAST_SIGNAL.get(base, 0) < COOLDOWN_SEC:
        return

    # === 2. Фильтр вершины (24h change) ===
    change_24h = item.get("change24", 0)
    if abs(change_24h) >= MAX_24H_CHANGE:
        STATS["rejected_24h"] += 1
        return

    # === 3. KuCoin 1m — триггер ===
    kc_candles = await fetch_kucoin_candles_1m(item["kucoin_symbol"])
    kc = calc_pump_metrics(kc_candles)
    if not kc:
        return

    if kc["rvol"] < TRIGGER_RVOL_1M:
        return
    if kc["move_pct"] < TRIGGER_MOVE_1M:
        return
    if kc["move_pct"] > TRIGGER_MAX_MOVE_1M:
        return
    if kc["volume_usd"] < TRIGGER_MIN_VOLUME_USD:
        return

    STATS["pump_triggers"] += 1
    log.info("🚨 PUMP TRIGGER: %s | RVOL=%.2fx move=+%.2f%% vol=$%.0f",
             base, kc["rvol"], kc["move_pct"], kc["volume_usd"])

    # === 4. Bitget + Bybit подтверждение ===
    bg_candles, bb_candles = await asyncio.gather(
        fetch_bitget_candles_1m(item["bitget_symbol"]),
        fetch_bybit_candles_1m(base),
    )

    bg = calc_pump_metrics(bg_candles)
    bb = calc_pump_metrics(bb_candles)

    confirmations = 0
    if bg and bg["rvol"] >= CONFIRM_RVOL and bg["move_pct"] >= CONFIRM_MOVE:
        confirmations += 1
    if bb and bb["rvol"] >= CONFIRM_RVOL and bb["move_pct"] >= CONFIRM_MOVE:
        confirmations += 1

    if confirmations < MIN_CONFIRMATIONS - 1:
        STATS["rejected_no_confirm"] += 1
        log.info("REJECT %s: confirmations=%d/%d", base, confirmations + 1, MIN_CONFIRMATIONS)
        return

    # === 5. OI по 3 биржам ===
    kc_oi, bg_oi, bb_oi = await asyncio.gather(
        fetch_kucoin_oi(item["kucoin_symbol"]),
        fetch_bitget_oi(item["bitget_symbol"]),
        fetch_bybit_oi(base),
    )

    now = time.time()
    hist = OI_HISTORY[base]
    if kc_oi > 0:
        hist.append((now, "kucoin", kc_oi))
    if bg_oi > 0:
        hist.append((now, "bitget", bg_oi))
    if bb_oi > 0:
        hist.append((now, "bybit", bb_oi))

    per_src = defaultdict(list)
    for ts, name, val in hist:
        per_src[name].append(val)

    oi_deltas = {}
    positive_sources = 0
    for name, arr in per_src.items():
        if len(arr) >= 2 and arr[0] > 0:
            d = ((arr[-1] / arr[0]) - 1) * 100
            oi_deltas[name] = d
            if d >= OI_MIN_GROWTH_PCT:
                positive_sources += 1
        else:
            oi_deltas[name] = 0.0

    if positive_sources < OI_MIN_SOURCES:
        STATS["rejected_no_oi"] += 1
        log.info("REJECT %s: OI sources=%d/%d deltas=%s",
                 base, positive_sources, OI_MIN_SOURCES, oi_deltas)
        return

    # === 6. СИГНАЛ ===
    LAST_SIGNAL[base] = time.time()
    await send_pump_signal(base, kc, bg, bb, oi_deltas, change_24h)

    log.info("✅ PUMP SIGNAL: %s | KC RVOL=%.1fx move=+%.2f%%",
             base, kc["rvol"], kc["move_pct"])


# ============================================================
# UNIVERSE
# ============================================================

async def refresh_universe():
    global UNIVERSE
    kc = await fetch_kucoin_contracts()
    bg = await fetch_bitget_tickers()
    if not kc or not bg:
        return

    bg_set = set(bg.keys())
    uni = {}
    for base, info in kc.items():
        if info["volume24"] < MIN_24H_VOLUME_USDT:
            continue
        if not (MIN_PRICE_USDT <= info["price"] <= MAX_PRICE_USDT):
            continue
        if base not in bg_set:
            continue
        uni[base] = {
            "kucoin_symbol": info["symbol"],
            "bitget_symbol": f"{base}USDT",
            "volume24": info["volume24"],
            "change24": info.get("change24", 0),
        }

    sorted_u = sorted(uni.items(), key=lambda x: x[1]["volume24"], reverse=True)
    UNIVERSE = dict(sorted_u[:MAX_UNIVERSE_SYMBOLS])
    log.info("🌐 Юниверс: %d пар", len(UNIVERSE))


# ============================================================
# SCAN
# ============================================================

async def scan_cycle():
    if not UNIVERSE:
        return
    STATS["scans"] += 1
    cands = list(UNIVERSE.keys())[:MAX_SCAN_CANDIDATES]

    semaphore = asyncio.Semaphore(8)

    async def worker(base):
        async with semaphore:
            try:
                await process_symbol(base)
                await asyncio.sleep(0.02)
            except Exception as e:
                log.exception("Error %s: %s", base, e)

    await asyncio.gather(*[worker(b) for b in cands])


async def main_loop():
    last_universe = 0
    while True:
        try:
            now = time.time()
            if now - last_universe > UNIVERSE_REFRESH_SEC:
                await refresh_universe()
                last_universe = now
            if UNIVERSE:
                await scan_cycle()
            await asyncio.sleep(SCAN_INTERVAL_SEC)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("Scanner error: %s", e)
            await asyncio.sleep(10)


# ============================================================
# WEB
# ============================================================

async def index(req):
    return web.Response(
        text=f"PUMP-HUNTER v9 | Uni: {len(UNIVERSE)} | "
             f"Triggers: {STATS['pump_triggers']} | Signals: {STATS['signals']} | "
             f"Scans: {STATS['scans']}",
        content_type="text/plain",
    )


async def start(app):
    global SESSION, HTTP_SEMAPHORE
    SESSION = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=50, ttl_dns_cache=300))
    HTTP_SEMAPHORE = asyncio.Semaphore(15)
    app["task"] = asyncio.create_task(main_loop())
    await send_tg(
        "🚀 <b>ПАМП-ХАНТЕР v9.0 запущен</b>\n\n"
        f"Триггер: KuCoin 1m RVOL ≥ {TRIGGER_RVOL_1M}x + move ≥ {TRIGGER_MOVE_1M}%\n"
        f"Подтверждение: Bitget + Bybit\n"
        f"OI: 2 из 3 бирж должны расти\n"
        f"Защита: 24h change < {MAX_24H_CHANGE}%"
    )


async def stop(app):
    t = app.get("task")
    if t:
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass
    if SESSION and not SESSION.closed:
        await SESSION.close()


app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/health", index)
app.on_startup.append(start)
app.on_cleanup.append(stop)


if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
