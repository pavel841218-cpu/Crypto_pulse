import asyncio
import aiohttp
from aiohttp import web
import os
import time
import logging
from collections import defaultdict, deque
import json


# ============================================================
#       CEX AGGREGATOR v5.0
#       Bitunix SCANNER → Bitget + Gate + Bybit CONFIRMATION
# ============================================================


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

PORT = int(os.environ.get("PORT", "10000"))

UNIVERSE_REFRESH_SEC = 300     # обновляем вселенную раз в 5 минут
CANDLE_REFRESH_SEC = 30        # цикл сканирования

MAX_UNIVERSE_SYMBOLS = 500     # сколько монет на Bitunix берём
MAX_BITUNIX_CANDIDATES = 200   # сколько монет проверяем на свечах

MIN_24H_VOLUME_USDT = 400_000
MIN_PRICE_USDT = 0.001
MAX_PRICE_USDT = 1.0
MIN_24H_CHANGE_PCT = 2.0

SIGNAL_COOLDOWN_SEC = 4 * 3600
HOURLY_LOG_SEC = 3600


# ============================================================
# Strategy thresholds
# ============================================================

# Bitunix — лидер (агрессивные пороги, ищем ранний импульс)
BITUNIX_MIN_RVOL = 3.0
BITUNIX_MIN_MOVE_PCT = 0.5
BITUNIX_MIN_BODY_RATIO = 0.45
BITUNIX_MIN_CLOSE_POSITION = 0.55

# Крупные биржи — подтверждение (мягче, они отстают)
CONFIRM_MIN_RVOL = 1.5
CONFIRM_MIN_MOVE_PCT = 0.3

# Минимум подтверждений от крупных бирж (из 3)
MIN_CONFIRMATIONS = 2

# OI агрегат
MIN_OI_GROWTH_PCT = 0.15

MIN_CANDLES = 20


# ============================================================
# URLs
# ============================================================

BITUNIX_BASE = "https://openapi.bitunix.com"
BITGET_BASE = "https://api.bitget.com"
GATE_BASE = "https://api.gateio.ws"
BYBIT_BASE = "https://api.bybit.com"


# ============================================================
# Logging & Runtime State
# ============================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("CEX-AGGREGATOR-v5.0")

START_TIME = time.time()
SESSION = None

UNIVERSE = {}            # base -> {bitunix_symbol, bitget_symbol, gate_symbol, bybit_symbol}
LAST_SIGNAL = {}
OI_HISTORY = defaultdict(lambda: deque(maxlen=20))
LAST_HOURLY_LOG = 0

STATS = {
    "bitunix_requests": 0, "bitget_requests": 0, "gate_requests": 0, "bybit_requests": 0,
    "bitunix_errors": 0, "bitget_errors": 0, "gate_errors": 0, "bybit_errors": 0,

    "bitunix_scans": 0,
    "bitunix_candidates": 0,
    "confirmed_signals": 0,

    "rejected_no_move": 0,
    "rejected_no_rvol": 0,
    "rejected_no_structure": 0,
    "rejected_not_confirmed": 0,
    "rejected_oi": 0,

    "oi_sources_used": 0,
}


# ============================================================
# HTTP
# ============================================================

async def http_get(url, params=None, timeout=8, service="other"):
    global SESSION
    key_req = f"{service}_requests"
    key_err = f"{service}_errors"

    try:
        if key_req in STATS:
            STATS[key_req] += 1

        async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            if resp.status == 429:
                log.warning("[%s] 429 rate limit", service)
                await asyncio.sleep(2.0)
                if key_err in STATS:
                    STATS[key_err] += 1
                return None
            if resp.status >= 400:
                if key_err in STATS:
                    STATS[key_err] += 1
                return None
            return await resp.json()
    except Exception as e:
        if key_err in STATS:
            STATS[key_err] += 1
        return None


# ============================================================
# Helpers
# ============================================================

def num(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def norm(symbol):
    if not symbol:
        return ""
    s = str(symbol).upper()
    for suf in ("USDTM", "USDT", "-USDT", "_USDT", "PERP"):
        if s.endswith(suf):
            s = s[:-len(suf)]
            break
    return s


# ============================================================
# Bitunix
# ============================================================

async def fetch_bitunix_tickers():
    """Тикеры Bitunix. Возвращает {base: {symbol, price, volume24}}"""
    url = f"{BITUNIX_BASE}/api/v1/futures/market/tickers"
    data = await http_get(url, service="bitunix")
    result = {}

    if not data:
        return result

    # Структура Bitunix может быть {'data': [...]} или [...]
    rows = data.get("data", []) if isinstance(data, dict) else data
    if not isinstance(rows, list):
        return result

    for row in rows:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol.endswith("USDT"):
            continue
        base = norm(symbol)
        if not base:
            continue

        price = num(row.get("lastPrice") or row.get("last"))
        vol = num(row.get("quoteVolume") or row.get("volume24") or row.get("baseVolume"))
        change = num(row.get("priceChangePercent") or row.get("change24h"))

        result[base] = {
            "symbol": symbol,
            "price": price,
            "volume24": vol,
            "change24": change,
        }

    return result


async def fetch_bitunix_candles(symbol):
    """5m свечи Bitunix."""
    url = f"{BITUNIX_BASE}/api/v1/futures/market/kline"
    params = {"symbol": symbol, "interval": "5m", "limit": 100}
    data = await http_get(url, params=params, service="bitunix")

    rows = data.get("data", []) if isinstance(data, dict) else data
    if not isinstance(rows, list):
        return []

    candles = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            ts = int(num(row.get("time") or row.get("ts") or row.get("t")))
            o = num(row.get("open") or row.get("o"))
            h = num(row.get("high") or row.get("h"))
            l = num(row.get("low") or row.get("l"))
            c = num(row.get("close") or row.get("c"))
            v = num(row.get("baseVolume") or row.get("volume") or row.get("v"))
            if o > 0 and c > 0:
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except Exception:
            continue

    candles.sort(key=lambda x: x["ts"])
    return candles


# ============================================================
# Bitget
# ============================================================

async def fetch_bitget_candles(symbol):
    url = f"{BITGET_BASE}/api/v2/mix/market/candles"
    params = {"symbol": symbol, "productType": "USDT-FUTURES", "granularity": "5m", "limit": "100"}
    data = await http_get(url, params=params, service="bitget")
    if not data or data.get("code") != "00000":
        return []

    rows = data.get("data", [])
    candles = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts, o, h, l, c, v = int(row[0]), num(row[1]), num(row[2]), num(row[3]), num(row[4]), num(row[5])
            if o > 0 and c > 0:
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except Exception:
            continue

    candles.sort(key=lambda x: x["ts"])
    return candles


async def fetch_bitget_oi(symbol):
    """OI Bitget. Использует open-interest endpoint."""
    url = f"{BITGET_BASE}/api/v2/mix/market/open-interest"
    params = {"symbol": symbol, "productType": "USDT-FUTURES"}
    data = await http_get(url, params=params, service="bitget")

    if not data or data.get("code") != "00000":
        return 0.0

    raw = data.get("data", {})
    row = raw
    if isinstance(raw, dict):
        lst = raw.get("list", [])
        if isinstance(lst, list) and lst:
            row = lst[0]
    elif isinstance(raw, list) and raw:
        row = raw[0]

    return num(
        row.get("amount") or row.get("openInterest") or row.get("size")
        or row.get("openInterestUsd") or row.get("holdingAmount")
    )


# ============================================================
# Gate.io
# ============================================================

async def fetch_gate_candles(symbol):
    """5m свечи Gate.io futures. symbol = BTC_USDT"""
    url = f"{GATE_BASE}/api/v4/futures/usdt/candlesticks"
    params = {"contract": symbol, "interval": "5m", "limit": 100}
    data = await http_get(url, params=params, service="gate")

    if not isinstance(data, list):
        return []

    candles = []
    for row in data:
        if not isinstance(row, dict):
            continue
        try:
            ts = int(num(row.get("t"))) * 1000
            o = num(row.get("o"))
            h = num(row.get("h"))
            l = num(row.get("l"))
            c = num(row.get("c"))
            v = num(row.get("v"))
            if o > 0 and c > 0:
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except Exception:
            continue

    candles.sort(key=lambda x: x["ts"])
    return candles


async def fetch_gate_oi(symbol):
    """OI Gate. symbol = BTC_USDT"""
    url = f"{GATE_BASE}/api/v4/futures/usdt/contract_stats"
    params = {"contract": symbol, "interval": "5m", "limit": 2}
    data = await http_get(url, params=params, service="gate")

    if not isinstance(data, list) or not data:
        return 0.0

    return num(data[-1].get("total_size") or data[-1].get("total_size_usd"))


# ============================================================
# Bybit
# ============================================================

async def fetch_bybit_candles(symbol):
    """5m свечи Bybit. symbol = BTCUSDT"""
    url = f"{BYBIT_BASE}/v5/market/kline"
    params = {"category": "linear", "symbol": symbol, "interval": "5", "limit": 100}
    data = await http_get(url, params=params, service="bybit")

    if not data or data.get("retCode") != 0:
        return []

    rows = data.get("result", {}).get("list", [])
    candles = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts = int(row[0])
            o = num(row[1])
            h = num(row[2])
            l = num(row[3])
            c = num(row[4])
            v = num(row[5])
            if o > 0 and c > 0:
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except Exception:
            continue

    candles.sort(key=lambda x: x["ts"])
    return candles


async def fetch_bybit_oi(symbol):
    url = f"{BYBIT_BASE}/v5/market/open-interest"
    params = {"category": "linear", "symbol": symbol, "intervalTime": "5min", "limit": 2}
    data = await http_get(url, params=params, service="bybit")

    if not data or data.get("retCode") != 0:
        return 0.0

    items = data.get("result", {}).get("list", [])
    if not items:
        return 0.0

    return num(items[0].get("openInterest"))


# ============================================================
# Candle metrics
# ============================================================

def calc_metrics(candles):
    """RVOL + move + body + close position на предпоследней свече."""
    if len(candles) < MIN_CANDLES:
        return None

    candle = candles[-2]
    prev = candles[:-2]

    close, open_, high, low = candle["close"], candle["open"], candle["high"], candle["low"]
    if open_ <= 0:
        return None

    vol_usd = candle["volume"] * close
    move_pct = ((close / open_) - 1) * 100
    rng = high - low
    if rng <= 0:
        return None

    body_ratio = abs(close - open_) / rng
    close_pos = (close - low) / rng

    vols = [(x["volume"] * x["close"]) for x in prev[-20:] if x["volume"] > 0]
    if not vols:
        return None

    avg = sum(vols) / len(vols)
    rvol = vol_usd / avg if avg > 0 else 0

    return {
        "close": close,
        "move_pct": move_pct,
        "rvol": rvol,
        "volume_usd": vol_usd,
        "body_ratio": body_ratio,
        "close_position": close_pos,
    }


# ============================================================
# Universe
# ============================================================

async def refresh_universe():
    """Bitunix как базовая биржа. Привязка к Bitget/Gate/Bybit."""
    global UNIVERSE

    bn = await fetch_bitunix_tickers()
    if not bn:
        log.warning("Bitunix tickers failed")
        return

    # Bitget — для маппинга
    bitget_url = f"{BITGET_BASE}/api/v2/mix/market/tickers"
    bitget_data = await http_get(bitget_url, params={"productType": "USDT-FUTURES"}, service="bitget")
    bitget_set = set()
    if bitget_data and bitget_data.get("code") == "00000":
        for r in bitget_data.get("data", []):
            bitget_set.add(norm(r.get("symbol", "")))

    universe = {}
    passed_vol = 0
    passed_price = 0
    passed_change = 0
    matched = 0

    for base, info in bn.items():
        if info["volume24"] < MIN_24H_VOLUME_USDT:
            continue
        passed_vol += 1
        if not (MIN_PRICE_USDT <= info["price"] <= MAX_PRICE_USDT):
            continue
        passed_price += 1
        ch = info.get("change24")
        if ch is not None and abs(ch) < MIN_24H_CHANGE_PCT:
            continue
        passed_change += 1

        if base not in bitget_set:
            continue
        matched += 1

        universe[base] = {
            "bitunix_symbol": info["symbol"],
            "bitget_symbol": f"{base}USDT",
            "gate_symbol": f"{base}_USDT",
            "bybit_symbol": f"{base}USDT",
            "price": info["price"],
            "volume24": info["volume24"],
            "change24": ch,
        }

    sorted_u = sorted(universe.items(), key=lambda x: x[1]["volume24"], reverse=True)
    UNIVERSE = dict(sorted_u[:MAX_UNIVERSE_SYMBOLS])

    log.info(
        "UNIVERSE | Bitunix total=%d | vol=%d price=%d change=%d | matched_bitget=%d | Common=%d",
        len(bn), passed_vol, passed_price, passed_change, matched, len(UNIVERSE)
    )


def select_candidates():
    items = list(UNIVERSE.items())
    items.sort(key=lambda x: abs(x[1].get("change24") or 0), reverse=True)
    return [b for b, _ in items[:MAX_BITUNIX_CANDIDATES]]


# ============================================================
# Telegram
# ============================================================

async def send_telegram(text):
    if not BOT_TOKEN or not CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        async with SESSION.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            return resp.status == 200
    except Exception:
        return False


async def send_signal(base, bn_metrics, confirmations, oi_data):
    conf_lines = []
    for name, m in confirmations.items():
        if m:
            conf_lines.append(f"  • {name}: RVOL {m['rvol']:.2f}x, move {m['move_pct']:+.2f}%")
        else:
            conf_lines.append(f"  • {name}: — нет данных")
    conf_block = "\n".join(conf_lines)

    oi_block = ""
    if oi_data and oi_data.get("sources", 0) > 0:
        oi_block = (
            f"\n📈 <b>OI (агрегат)</b>\n"
            f"Источники: {oi_data['sources']}\n"
            f"Макс. ΔOI: <b>{oi_data['delta_pct']:+.2f}%</b>\n"
        )

    msg = (
        f"🚀 <b>LEAD-LAG SIGNAL: {base}</b>\n\n"
        f"🎯 <b>Bitunix (лидер)</b>\n"
        f"Move 5m: {bn_metrics['move_pct']:+.2f}%\n"
        f"RVOL: <b>{bn_metrics['rvol']:.2f}x</b>\n"
        f"Объём: ${bn_metrics['volume_usd']:,.0f}\n"
        f"Close position: {bn_metrics['close_position']:.2f}\n\n"
        f"✅ <b>Подтверждение (крупные CEX)</b>\n{conf_block}\n"
        f"{oi_block}\n"
        f"⚠️ Bitunix показал аномалию раньше. Заходим до отработки на крупных биржах!"
    )
    await send_telegram(msg)


# ============================================================
# Process symbol
# ============================================================

async def process_symbol(base):
    item = UNIVERSE.get(base)
    if not item:
        return

    STATS["bitunix_scans"] += 1

    # 1. Bitunix — сканер
    bn_candles = await fetch_bitunix_candles(item["bitunix_symbol"])
    bn = calc_metrics(bn_candles)
    if not bn:
        return

    if bn["move_pct"] < BITUNIX_MIN_MOVE_PCT:
        STATS["rejected_no_move"] += 1
        return
    if bn["rvol"] < BITUNIX_MIN_RVOL:
        STATS["rejected_no_rvol"] += 1
        return
    if bn["body_ratio"] < BITUNIX_MIN_BODY_RATIO or bn["close_position"] < BITUNIX_MIN_CLOSE_POSITION:
        STATS["rejected_no_structure"] += 1
        return

    STATS["bitunix_candidates"] += 1
    log.info("Bitunix CANDIDATE: %s | RVOL=%.2fx move=%+.2f%%",
             base, bn["rvol"], bn["move_pct"])

    # 2. Подтверждение крупных бирж — параллельно
    bg_t, gate_t, bb_t = await asyncio.gather(
        fetch_bitget_candles(item["bitget_symbol"]),
        fetch_gate_candles(item["gate_symbol"]),
        fetch_bybit_candles(item["bybit_symbol"]),
    )

    bg_m = calc_metrics(bg_t)
    gate_m = calc_metrics(gate_t)
    bb_m = calc_metrics(bb_t)

    confirmations = {"Bitget": bg_m, "Gate": gate_m, "Bybit": bb_m}

    confirmed_count = 0
    for m in (bg_m, gate_m, bb_m):
        if m and m["rvol"] >= CONFIRM_MIN_RVOL and m["move_pct"] >= CONFIRM_MIN_MOVE_PCT:
            confirmed_count += 1

    if confirmed_count < MIN_CONFIRMATIONS:
        STATS["rejected_not_confirmed"] += 1
        log.info("REJECT %s: confirmations=%d/%d", base, confirmed_count, MIN_CONFIRMATIONS)
        return

    # 3. OI агрегат: Bitget + Gate + Bybit
    bg_oi, gate_oi, bb_oi = await asyncio.gather(
        fetch_bitget_oi(item["bitget_symbol"]),
        fetch_gate_oi(item["gate_symbol"]),
        fetch_bybit_oi(item["bybit_symbol"]),
    )

    now = time.time()
    hist = OI_HISTORY[base]

    oi_sources = 0
    max_delta = 0.0

    for name, val in (("bitget", bg_oi), ("gate", gate_oi), ("bybit", bb_oi)):
        if val > 0:
            oi_sources += 1
            hist.append((now, name, val))

    # считаем max дельту по каждому источнику
    per_src = defaultdict(list)
    for ts, name, val in hist:
        per_src[name].append(val)

    for name, arr in per_src.items():
        if len(arr) >= 2 and arr[0] > 0:
            d = ((arr[-1] / arr[0]) - 1) * 100
            if d > max_delta:
                max_delta = d

    oi_data = {"sources": oi_sources, "delta_pct": max_delta}

    if oi_sources > 0 and max_delta < MIN_OI_GROWTH_PCT:
        STATS["rejected_oi"] += 1
        log.info("REJECT %s: OI delta=%.2f%% < %.2f%%", base, max_delta, MIN_OI_GROWTH_PCT)
        return

    if oi_sources > 0:
        STATS["oi_sources_used"] += 1

    # 4. Cooldown и сигнал
    if time.time() - LAST_SIGNAL.get(base, 0) < SIGNAL_COOLDOWN_SEC:
        return

    LAST_SIGNAL[base] = time.time()
    STATS["confirmed_signals"] += 1

    await send_signal(base, bn, confirmations, oi_data)


# ============================================================
# Scan cycle
# ============================================================

async def scan_cycle():
    if not UNIVERSE:
        return

    candidates = select_candidates()
    log.info("SCAN cycle | candidates=%d", len(candidates))

    semaphore = asyncio.Semaphore(5)

    async def worker(base):
        async with semaphore:
            try:
                await process_symbol(base)
                await asyncio.sleep(0.05)
            except Exception as e:
                log.exception("Error %s: %s", base, e)

    await asyncio.gather(*[worker(base) for base in candidates])


# ============================================================
# Hourly diagnostics
# ============================================================

async def hourly_diagnostics():
    global LAST_HOURLY_LOG
    now = time.time()
    if now - LAST_HOURLY_LOG < HOURLY_LOG_SEC:
        return
    LAST_HOURLY_LOG = now

    log.info(
        "HOURLY | universe=%d | BN scans=%d cand=%d signals=%d | "
        "rej: move=%d rvol=%d struct=%d conf=%d oi=%d",
        len(UNIVERSE),
        STATS["bitunix_scans"], STATS["bitunix_candidates"], STATS["confirmed_signals"],
        STATS["rejected_no_move"], STATS["rejected_no_rvol"],
        STATS["rejected_no_structure"], STATS["rejected_not_confirmed"],
        STATS["rejected_oi"],
    )

    msg = (
        f"🩺 <b>ЧАСОВАЯ ДИАГНОСТИКА v5.0</b>\n\n"
        f"<b>Юниверс:</b> {len(UNIVERSE)} монет\n"
        f"<b>Bitunix сканов:</b> {STATS['bitunix_scans']}\n"
        f"<b>Bitunix кандидатов:</b> {STATS['bitunix_candidates']}\n"
        f"<b>Сигналов:</b> {STATS['confirmed_signals']}\n\n"
        f"<b>Отсевы</b>\n"
        f"Нет движения: {STATS['rejected_no_move']}\n"
        f"Низкий RVOL: {STATS['rejected_no_rvol']}\n"
        f"Структура: {STATS['rejected_no_structure']}\n"
        f"Нет подтверждений: {STATS['rejected_not_confirmed']}\n"
        f"OI низкий: {STATS['rejected_oi']}\n\n"
        f"<b>HTTP</b>\n"
        f"BNX {STATS['bitunix_requests']}/{STATS['bitunix_errors']} | "
        f"BG {STATS['bitget_requests']}/{STATS['bitget_errors']} | "
        f"Gate {STATS['gate_requests']}/{STATS['gate_errors']} | "
        f"BB {STATS['bybit_requests']}/{STATS['bybit_errors']}"
    )
    await send_telegram(msg)


# ============================================================
# Main loop
# ============================================================

async def scanner_loop():
    last_universe = 0
    while True:
        try:
            now = time.time()
            if now - last_universe >= UNIVERSE_REFRESH_SEC:
                await refresh_universe()
                last_universe = now

            if UNIVERSE:
                await scan_cycle()

            await hourly_diagnostics()
            await asyncio.sleep(CANDLE_REFRESH_SEC)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("Scanner error: %s", e)
            await asyncio.sleep(10)


# ============================================================
# Web
# ============================================================

async def index(request):
    return web.Response(
        text=f"CEX AGGREGATOR v5.0 | Universe: {len(UNIVERSE)} | Signals: {STATS['confirmed_signals']}",
        content_type="text/plain"
    )


async def health(request):
    return web.Response(text="ok")


async def start_background(app):
    global SESSION
    SESSION = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=30, ttl_dns_cache=300))
    app["scanner_task"] = asyncio.create_task(scanner_loop())
    await send_telegram(
        "🚀 <b>CEX AGGREGATOR v5.0</b>\n"
        "• <b>Bitunix = лидер</b> (сканер аномалий)\n"
        "• Bitget + Gate + Bybit = подтверждение\n"
        "• OI агрегат по 3 CEX\n"
        f"• Порог Bitunix: RVOL ≥ {BITUNIX_MIN_RVOL}x, move ≥ {BITUNIX_MIN_MOVE_PCT}%"
    )


async def cleanup(app):
    t = app.get("scanner_task")
    if t:
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass
    global SESSION
    if SESSION:
        await SESSION.close()


app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/stats", index)
app.router.add_get("/health", health)
app.on_startup.append(start_background)
app.on_cleanup.append(cleanup)


if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
