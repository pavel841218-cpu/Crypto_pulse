import asyncio
import aiohttp
from aiohttp import web
import os
import time
import logging
from collections import defaultdict, deque

# ============================================================
# CEX AGGREGATOR v7.4
# OI-FIRST TRIGGER + 6 CEX + LIVE 1H + EARLY STAGE
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
PORT = int(os.environ.get("PORT", "10000"))

UNIVERSE_REFRESH_SEC = 300
CANDLE_REFRESH_SEC = 20

MAX_UNIVERSE_SYMBOLS = 400
MAX_SCAN_CANDIDATES = 150

MIN_24H_VOLUME_USDT = 300_000
MIN_PRICE_USDT = 0.0001
MAX_PRICE_USDT = 10.0

SIGNAL_COOLDOWN_SEC = 4 * 3600
WATCHLIST_TTL_SEC = 6 * 3600
WATCHLIST_MAX_SIZE = 60

# === OI-FIRST TRIGGER ===
OI_ANOMALY_PCT = 2.5              # рост OI за lookback
OI_MIN_LOOKBACK_SNAPS = 15        # 15 снимков × 20 сек = 5 мин
OI_MAX_LOOKBACK_SNAPS = 45        # не больше 15 мин в истории

# === RVOL CONFIRMATION (после OI) ===
SIGNAL_RVOL_KUCOIN = 1.5
SIGNAL_RVOL_BITGET = 1.3
SIGNAL_MIN_MOVE_KUCOIN = 0.3
SIGNAL_MIN_MOVE_BITGET = 0.2

# === EARLY STAGE FILTER ===
MAX_24H_CHANGE_FOR_LONG = 15.0
MAX_MOVE_LAST_30MIN_PCT = 12.0
DISTRIBUTION_DROP_PCT = 2.0

# === LIVE 1H ===
LIVE_MIN_MINUTES = 3
LIVE_MAX_MINUTES = 50

# === OI DIVERGENCE ===
OI_DIVERGENCE_MIN_PCT = 0.5
MIN_OI_SOURCES = 2

MIN_CANDLES = 20

KUCOIN_BASE = "https://api-futures.kucoin.com"
BITGET_BASE = "https://api.bitget.com"
BYBIT_BASE = "https://api.bybit.com"
OKX_BASE = "https://www.okx.com"
GATE_BASE = "https://api.gateio.ws"
BINGX_BASE = "https://open-api.bingx.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("CEX-AGGREGATOR-v7.4")

SESSION = None
UNIVERSE = {}
WATCHLIST = {}      # теперь watchlist = монеты с аномальным OI
LAST_SIGNAL = {}
LAST_UPDATE_ID = 0

# Храним короткие снимки KuCoin OI для каждой монеты
KUCOIN_OI_HISTORY = defaultdict(lambda: deque(maxlen=90))
# Полные снимки OI по 6 биржам (только для монет в watchlist)
FULL_OI_SNAPSHOTS = defaultdict(lambda: deque(maxlen=30))

STATS = {
    "kucoin_requests": 0, "bitget_requests": 0, "bybit_requests": 0,
    "okx_requests": 0, "gate_requests": 0, "bingx_requests": 0,
    "scans": 0, "oi_scans": 0,
    "oi_anomalies_found": 0,
    "watchlist_added": 0, "watchlist_expired": 0,
    "confirmed_signals": 0, "amplified_signals": 0,
    "rejected_24h_change": 0,
    "rejected_late_entry": 0,
    "rejected_distribution": 0,
    "rejected_no_rvol_kc": 0,
    "rejected_no_rvol_bg": 0,
    "rejected_oi_all_negative": 0,
    "manual_checks": 0,
    "update_errors": 0,
}


# ============================================================
# HTTP
# ============================================================

async def http_get(url, params=None, timeout=6, service="other"):
    global SESSION
    key_req = f"{service}_requests"
    try:
        if key_req in STATS:
            STATS[key_req] += 1
        async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            if resp.status == 429:
                await asyncio.sleep(1.0)
                return None
            if resp.status >= 400:
                return None
            return await resp.json()
    except Exception:
        return None


def num(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def norm(symbol):
    if not symbol:
        return ""
    s = str(symbol).upper()
    if s.startswith("XBT"):
        s = "BTC" + s[3:]
    for suf in ("USDTM", "USDT", "-USDT", "_USDT", "PERP"):
        if s.endswith(suf):
            s = s[:-len(suf)]
            break
    return s


# ============================================================
# CEX FETCHERS
# ============================================================

async def fetch_kucoin_contracts():
    url = f"{KUCOIN_BASE}/api/v1/contracts/active"
    data = await http_get(url, service="kucoin")
    result = {}
    if not data or not isinstance(data.get("data"), list):
        return result
    for row in data["data"]:
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


async def fetch_kucoin_oi(symbol):
    """KuCoin OI через ticker (быстрый и надёжный)."""
    url = f"{KUCOIN_BASE}/api/v1/ticker"
    data = await http_get(url, params={"symbol": symbol}, service="kucoin")
    if not data or not isinstance(data.get("data"), dict):
        return 0.0
    return num(data["data"].get("openInterest"))


async def fetch_kucoin_candles(symbol, granularity="5"):
    url = f"{KUCOIN_BASE}/api/v1/kline/query"
    data = await http_get(url, params={"symbol": symbol, "granularity": granularity}, service="kucoin")
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
        except Exception:
            continue
    candles.sort(key=lambda x: x["ts"])
    return candles


async def fetch_bitget_tickers():
    url = f"{BITGET_BASE}/api/v2/mix/market/tickers"
    data = await http_get(url, params={"productType": "USDT-FUTURES"}, service="bitget")
    result = {}
    if not data or data.get("code") != "00000":
        return result
    for row in data.get("data", []):
        symbol = str(row.get("symbol", "")).upper()
        if not symbol.endswith("USDT"):
            continue
        base = norm(symbol)
        if base:
            result[base] = {"symbol": symbol}
    return result


async def fetch_bitget_candles(symbol):
    url = f"{BITGET_BASE}/api/v2/mix/market/candles"
    params = {"symbol": symbol, "productType": "USDT-FUTURES", "granularity": "5m", "limit": "100"}
    data = await http_get(url, params=params, service="bitget")
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
        except Exception:
            continue
    candles.sort(key=lambda x: x["ts"])
    return candles


async def fetch_bitget_oi(symbol):
    url = f"{BITGET_BASE}/api/v2/mix/market/open-interest"
    data = await http_get(url, params={"symbol": symbol, "productType": "USDT-FUTURES"}, service="bitget")
    if data and data.get("code") == "00000":
        raw = data.get("data", {})
        row = raw.get("list", [{}])[0] if isinstance(raw, dict) and raw.get("list") else raw
        return num(row.get("amount") or row.get("openInterest") or row.get("openInterestUsd"))
    return 0.0


async def fetch_bingx_oi(symbol):
    url = f"{BINGX_BASE}/openApi/swap/v2/quote/ticker"
    data = await http_get(url, params={"symbol": f"{symbol}-USDT"}, service="bingx")
    if data and data.get("code") == 0:
        return num(data.get("data", {}).get("openInterest"))
    return 0.0


async def fetch_okx_oi(symbol):
    url = f"{OKX_BASE}/api/v5/public/open-interest"
    data = await http_get(url, params={"instType": "SWAP", "instId": f"{symbol}-USDT-SWAP"}, service="okx")
    if data and data.get("code") == "0":
        rows = data.get("data", [])
        return num(rows[0].get("oi")) if rows else 0.0
    return 0.0


async def fetch_gate_oi(symbol):
    url = f"{GATE_BASE}/api/v4/futures/usdt/contract_stats"
    data = await http_get(url, params={"contract": f"{symbol}_USDT", "interval": "5m", "limit": 1}, service="gate")
    if isinstance(data, list) and data:
        return num(data[-1].get("total_size"))
    return 0.0


async def fetch_bybit_oi(symbol):
    url = f"{BYBIT_BASE}/v5/market/open-interest"
    data = await http_get(url, params={"category": "linear", "symbol": f"{symbol}USDT", "intervalTime": "5min", "limit": 1}, service="bybit")
    if data and data.get("retCode") == 0:
        items = data.get("result", {}).get("list", [])
        return num(items[0].get("openInterest")) if items else 0.0
    return 0.0


# ============================================================
# OI HELPERS
# ============================================================

def snapshot_oi_kucoin(base, oi_value):
    """Добавляет снимок KuCoin OI в историю."""
    if oi_value > 0:
        KUCOIN_OI_HISTORY[base].append((time.time(), oi_value))


def get_kucoin_oi_delta(base, lookback_snaps=OI_MIN_LOOKBACK_SNAPS):
    """Считает дельту KuCoin OI за lookback_snaps снимков."""
    hist = KUCOIN_OI_HISTORY[base]
    if len(hist) < lookback_snaps:
        return 0.0
    old = hist[-lookback_snaps][1]
    new = hist[-1][1]
    if old <= 0:
        return 0.0
    return ((new / old) - 1) * 100


async def collect_all_oi(base):
    """Полный снимок OI по 6 биржам."""
    item = UNIVERSE.get(base)
    kc_sym = item["kucoin_symbol"] if item else f"{base}USDTM"

    kc, bg, bx, okx, gate, bb = await asyncio.gather(
        fetch_kucoin_oi(kc_sym),
        fetch_bitget_oi(f"{base}USDT"),
        fetch_bingx_oi(base),
        fetch_okx_oi(base),
        fetch_gate_oi(base),
        fetch_bybit_oi(base),
    )
    return {"kucoin": kc, "bitget": bg, "bingx": bx, "okx": okx, "gate": gate, "bybit": bb}


def calculate_full_oi_deltas(base, current_snapshot, lookback=10):
    now = time.time()
    snapshots = FULL_OI_SNAPSHOTS[base]
    snapshots.append((now, current_snapshot.copy()))

    if len(snapshots) < lookback:
        return {ex: 0.0 for ex in current_snapshot}

    _, old_snap = snapshots[-lookback]
    deltas = {}
    for ex, cur in current_snapshot.items():
        old = old_snap.get(ex, 0.0)
        if cur > 0 and old > 0:
            deltas[ex] = ((cur / old) - 1.0) * 100.0
        else:
            deltas[ex] = 0.0
    return deltas


def analyze_oi_divergence(deltas):
    valid = {k: v for k, v in deltas.items() if abs(v) > 0.01}
    positive = {k: v for k, v in valid.items() if v > 0}
    negative = {k: v for k, v in valid.items() if v < 0}
    sources = len(valid)
    max_pos = max(positive.values()) if positive else 0.0
    min_neg = min(negative.values()) if negative else 0.0
    divergence = sources >= MIN_OI_SOURCES and bool(positive) and bool(negative)
    amplifier = divergence and (max_pos - min_neg) >= (OI_DIVERGENCE_MIN_PCT * 2)
    return {
        "positive": positive, "negative": negative,
        "divergence": divergence, "amplifier": amplifier,
        "max_positive": max_pos, "min_negative": min_neg,
        "sources": sources,
    }


# ============================================================
# CANDLE METRICS + EARLY STAGE FILTER
# ============================================================

def calc_metrics(candles):
    if len(candles) < MIN_CANDLES:
        return None
    candle, prev = candles[-2], candles[:-2]
    close, open_, high, low = candle["close"], candle["open"], candle["high"], candle["low"]
    if open_ <= 0:
        return None
    vol_usd = candle["volume"] * close
    move_pct = ((close / open_) - 1) * 100
    rng = high - low
    body_ratio = abs(close - open_) / rng if rng > 0 else 1.0
    vols = [(x["volume"] * x["close"]) for x in prev[-20:] if x["volume"] > 0]
    if not vols:
        return None
    avg = sum(vols) / len(vols)
    rvol = vol_usd / avg if avg > 0 else 0
    return {
        "close": close, "move_pct": move_pct, "rvol": rvol,
        "volume_usd": vol_usd, "body_ratio": body_ratio,
    }


def is_distribution(candles):
    if len(candles) < 6:
        return False
    recent = candles[-6:]
    max_high = max(c["high"] for c in recent)
    current_close = candles[-2]["close"]
    if max_high <= 0:
        return False
    drop = ((current_close / max_high) - 1) * 100
    return drop <= -DISTRIBUTION_DROP_PCT


def early_stage_check(base, kc_candles):
    """Фильтр early stage. Возвращает (ok, reason)."""
    item = UNIVERSE.get(base, {})
    change_24h = item.get("change24", 0)

    if abs(change_24h) >= MAX_24H_CHANGE_FOR_LONG:
        return False, f"24h change {change_24h:+.2f}%"

    if len(kc_candles) >= 6:
        recent = kc_candles[-6:]
        first_open = recent[0]["open"]
        last_close = recent[-1]["close"]
        if first_open > 0:
            move_30m = ((last_close / first_open) - 1) * 100
            if move_30m >= MAX_MOVE_LAST_30MIN_PCT:
                return False, f"30m move {move_30m:+.2f}%"

    if is_distribution(kc_candles):
        return False, "distribution"

    return True, "ok"


# ============================================================
# WATCHLIST
# ============================================================

def add_to_watchlist(base, oi_delta, snapshot):
    now = time.time()
    if base in WATCHLIST:
        WATCHLIST[base]["expires_at"] = now + WATCHLIST_TTL_SEC
        WATCHLIST[base]["last_oi_delta"] = oi_delta
        return False
    if len(WATCHLIST) >= WATCHLIST_MAX_SIZE:
        oldest = min(WATCHLIST.items(), key=lambda x: x[1]["added_at"])[0]
        WATCHLIST.pop(oldest, None)
    WATCHLIST[base] = {
        "added_at": now,
        "expires_at": now + WATCHLIST_TTL_SEC,
        "oi_delta": oi_delta,
        "snapshot": snapshot,
    }
    STATS["watchlist_added"] += 1
    log.info(
        "🎯 OI-аномалия: %s | ΔOI KuCoin = +%.2f%% | snapshot: KC=%.0f BG=%.0f",
        base, oi_delta,
        snapshot.get("kucoin", 0), snapshot.get("bitget", 0),
    )
    return True


def cleanup_watchlist():
    now = time.time()
    expired = [b for b, w in WATCHLIST.items() if w["expires_at"] <= now]
    for b in expired:
        WATCHLIST.pop(b, None)
        STATS["watchlist_expired"] += 1


# ============================================================
# TELEGRAM
# ============================================================

async def send_telegram(text, parse_mode="HTML"):
    if not BOT_TOKEN or not CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID, "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    try:
        async with SESSION.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            return resp.status == 200
    except Exception:
        return False


async def send_oi_watch_alert(base, oi_delta, snapshot):
    """Уведомление: OI-аномалия, монета на карандаше."""
    snap_lines = "\n".join([
        f"  • {k.upper()}: <code>{v:,.0f}</code>"
        for k, v in snapshot.items() if v > 0
    ])
    msg = (
        f"👀 <b>OI-АНОМАЛИЯ: {base}USDT</b>\n"
        f"Монета взята на карандаш\n\n"
        f"📈 ΔOI KuCoin: <b>+{oi_delta:.2f}%</b>\n\n"
        f"<b>Текущий OI по биржам:</b>\n{snap_lines}\n\n"
        f"⏳ Ждём подтверждения RVOL..."
    )
    await send_telegram(msg)


async def send_signal(base, kc, bg, div, watchlist_info, oi_delta):
    hold_min = int((time.time() - watchlist_info.get("added_at", time.time())) / 60)
    item = UNIVERSE.get(base, {})
    change_24h = item.get("change24", 0)

    if div["amplifier"]:
        signal_type = "🔥🔥🔥 МАКСИМАЛЬНЫЙ (расхождение OI)"
    elif div["divergence"]:
        signal_type = "🔥 УСИЛЕННЫЙ (дисбаланс OI)"
    else:
        signal_type = "✅ ПОДТВЕРЖДЁН (рост OI + RVOL)"

    pos = ", ".join([f"{k.upper()}: +{v:.2f}%" for k, v in div["positive"].items()]) or "нет"
    neg = ", ".join([f"{k.upper()}: {v:.2f}%" for k, v in div["negative"].items()]) or "нет"

    msg = (
        f"🚀 <b>СИГНАЛ: {base}USDT</b>\n"
        f"{signal_type}\n\n"
        f"📌 OI-аномалия → RVOL подтверждение\n"
        f"⏱ В фокусе: {hold_min} мин\n"
        f"📊 24h change: <b>{change_24h:+.2f}%</b>\n\n"
        f"<b>1️⃣ OI (первичный триггер)</b>\n"
        f"ΔOI KuCoin: <b>+{oi_delta:.2f}%</b>\n"
        f"📈 Приток: <code>{pos}</code>\n"
        f"🔻 Сброс: <code>{neg}</code>\n\n"
        f"<b>2️⃣ RVOL (подтверждение)</b>\n"
        f"🎯 KuCoin: Move +{kc['move_pct']:.2f}% | RVOL <b>{kc['rvol']:.2f}x</b>\n"
        f"✅ Bitget: Move +{bg['move_pct']:.2f}% | RVOL <b>{bg['rvol']:.2f}x</b>"
    )
    await send_telegram(msg)


# ============================================================
# CORE: OI-FIRST SCAN
# ============================================================

async def oi_scan_one(base, semaphore):
    """Шаг 1: быстрый OI-скан KuCoin для одной монеты."""
    async with semaphore:
        item = UNIVERSE.get(base)
        if not item:
            return

        # Ранний фильтр: 24h change
        if abs(item.get("change24", 0)) >= MAX_24H_CHANGE_FOR_LONG:
            STATS["rejected_24h_change"] += 1
            return

        # Быстрый запрос OI KuCoin
        oi = await fetch_kucoin_oi(item["kucoin_symbol"])
        if oi <= 0:
            return

        snapshot_oi_kucoin(base, oi)

        # Считаем дельту
        delta = get_kucoin_oi_delta(base)
        if delta < OI_ANOMALY_PCT:
            return

        STATS["oi_anomalies_found"] += 1
        log.info("OI ANOMALY: %s | ΔOI KuCoin = +%.2f%%", base, delta)

        # Уже в watchlist?
        if base in WATCHLIST:
            WATCHLIST[base]["expires_at"] = time.time() + WATCHLIST_TTL_SEC
            WATCHLIST[base]["last_oi_delta"] = delta
            return

        # Собираем полный снимок по 6 биржам
        full = await collect_all_oi(base)
        add_to_watchlist(base, delta, full)


async def check_watchlist_candidate(base, semaphore):
    """
    Шаг 2: монета в watchlist. Проверяем RVOL + движение.
    Если подтверждено — сигнал.
    """
    async with semaphore:
        item = UNIVERSE.get(base)
        if not item:
            return

        # Свечи KuCoin + Bitget
        kc_candles, bg_candles = await asyncio.gather(
            fetch_kucoin_candles(item["kucoin_symbol"]),
            fetch_bitget_candles(item["bitget_symbol"]),
        )

        # Early stage фильтр
        ok, reason = early_stage_check(base, kc_candles)
        if not ok:
            if reason.startswith("24h"):
                STATS["rejected_24h_change"] += 1
            elif reason.startswith("30m"):
                STATS["rejected_late_entry"] += 1
            elif reason == "distribution":
                STATS["rejected_distribution"] += 1
            log.info("REJECT %s: %s", base, reason)
            WATCHLIST.pop(base, None)
            return

        kc = calc_metrics(kc_candles)
        bg = calc_metrics(bg_candles)
        if not kc or not bg:
            return

        # RVOL подтверждение
        if kc["rvol"] < SIGNAL_RVOL_KUCOIN or kc["move_pct"] < SIGNAL_MIN_MOVE_KUCOIN:
            STATS["rejected_no_rvol_kc"] += 1
            return
        if bg["rvol"] < SIGNAL_RVOL_BITGET or bg["move_pct"] < SIGNAL_MIN_MOVE_BITGET:
            STATS["rejected_no_rvol_bg"] += 1
            return

        # Полный снимок OI + дельты
        full = await collect_all_oi(base)
        deltas = calculate_full_oi_deltas(base, full, lookback=10)
        div = analyze_oi_divergence(deltas)

        # Если все OI падают — уже поздно
        if not div["positive"] and div["negative"]:
            STATS["rejected_oi_all_negative"] += 1
            log.info("REJECT %s: все OI падают", base)
            WATCHLIST.pop(base, None)
            return

        # Cooldown
        if time.time() - LAST_SIGNAL.get(base, 0) < SIGNAL_COOLDOWN_SEC:
            return

        LAST_SIGNAL[base] = time.time()
        STATS["confirmed_signals"] += 1
        if div["amplifier"]:
            STATS["amplified_signals"] += 1

        oi_delta = WATCHLIST.get(base, {}).get("last_oi_delta", 0)
        await send_signal(base, kc, bg, div, WATCHLIST.get(base, {}), oi_delta)
        WATCHLIST.pop(base, None)


# ============================================================
# SCAN CYCLE
# ============================================================

async def scan_cycle():
    if not UNIVERSE:
        return
    STATS["scans"] += 1
    candidates = list(UNIVERSE.keys())[:MAX_SCAN_CANDIDATES]
    semaphore = asyncio.Semaphore(8)

    # ШАГ 1: OI-скан по всем монетам
    await asyncio.gather(*[oi_scan_one(b, semaphore) for b in candidates])
    STATS["oi_scans"] += 1

    # ШАГ 2: проверяем watchlist (OI-аномалии)
    if WATCHLIST:
        active = list(WATCHLIST.keys())
        log.info("🎯 Watchlist: %d монет | проверяем RVOL...", len(active))
        await asyncio.gather(*[check_watchlist_candidate(b, semaphore) for b in active])

    cleanup_watchlist()


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
            await asyncio.sleep(CANDLE_REFRESH_SEC)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("Scanner error: %s", e)
            await asyncio.sleep(10)


# ============================================================
# UNIVERSE
# ============================================================

async def refresh_universe():
    global UNIVERSE
    kc = await fetch_kucoin_contracts()
    bitget = await fetch_bitget_tickers()
    if not kc or not bitget:
        log.warning("Universe refresh failed")
        return

    bitget_set = set(bitget.keys())
    universe = {}
    for base, info in kc.items():
        if info["volume24"] < MIN_24H_VOLUME_USDT:
            continue
        if not (MIN_PRICE_USDT <= info["price"] <= MAX_PRICE_USDT):
            continue
        if base not in bitget_set:
            continue
        universe[base] = {
            "kucoin_symbol": info["symbol"],
            "bitget_symbol": f"{base}USDT",
            "price": info["price"],
            "volume24": info["volume24"],
            "change24": info["change24"],
        }

    sorted_u = sorted(universe.items(), key=lambda x: x[1]["volume24"], reverse=True)
    UNIVERSE = dict(sorted_u[:MAX_UNIVERSE_SYMBOLS])
    log.info("🌐 Юниверс: %d монет", len(UNIVERSE))


# ============================================================
# MANUAL CHECK + TELEGRAM COMMANDS
# ============================================================

async def full_market_report(base):
    base = norm(base)
    item = UNIVERSE.get(base)
    kc_sym = item["kucoin_symbol"] if item else f"{base}USDTM"

    kc_c, bg_c, oi_all = await asyncio.gather(
        fetch_kucoin_candles(kc_sym),
        fetch_bitget_candles(f"{base}USDT"),
        collect_all_oi(base),
    )

    kc = calc_metrics(kc_c)
    bg = calc_metrics(bg_c)

    lines = [f"🔎 <b>ПОЛНАЯ СВЕРКА: {base}USDT</b>\n"]

    if kc:
        lines.append(
            f"🎯 <b>KuCoin</b>\n"
            f"  Цена: <code>{kc['close']:.8g}</code>\n"
            f"  Move 5m: <b>+{kc['move_pct']:.2f}%</b>\n"
            f"  RVOL: <b>{kc['rvol']:.2f}x</b>\n"
            f"  Объём: ${kc['volume_usd']:,.0f}"
        )
    else:
        lines.append("🎯 <b>KuCoin</b>: ❌ нет данных")

    if bg:
        lines.append(
            f"\n✅ <b>Bitget</b>\n"
            f"  Цена: <code>{bg['close']:.8g}</code>\n"
            f"  Move 5m: <b>+{bg['move_pct']:.2f}%</b>\n"
            f"  RVOL: <b>{bg['rvol']:.2f}x</b>\n"
            f"  Объём: ${bg['volume_usd']:,.0f}"
        )
    else:
        lines.append("\n✅ <b>Bitget</b>: ❌ нет данных")

    labels = {"kucoin": "KuCoin", "bitget": "Bitget", "bingx": "BingX",
              "okx": "OKX", "gate": "Gate.io", "bybit": "Bybit"}

    lines.append("\n📊 <b>OI по всем 6 биржам</b>")
    for ex, val in oi_all.items():
        label = labels.get(ex, ex)
        if val > 0:
            lines.append(f"  • {label}: <code>{val:,.0f}</code>")
        else:
            lines.append(f"  • {label}: ❌")

    # Дельта KuCoin из памяти
    kc_delta = get_kucoin_oi_delta(base)
    if abs(kc_delta) > 0.01:
        lines.append(f"\n📈 ΔOI KuCoin (5 мин): <b>{kc_delta:+.2f}%</b>")

    # Дивергенция (если есть история)
    full_deltas = calculate_full_oi_deltas(base, oi_all, lookback=5)
    div = analyze_oi_divergence(full_deltas)
    if div["sources"] >= 2:
        lines.append("\n📊 <b>ΔOI (короткий lookback)</b>")
        for ex, d in full_deltas.items():
            label = labels.get(ex, ex)
            sign = "🟢" if d > 0 else ("🔴" if d < 0 else "⚪")
            lines.append(f"  • {label}: {sign} <b>{d:+.2f}%</b>")
        if div["amplifier"]:
            lines.append("\n🔥 <b>Дивергенция OI</b> — возможен шорт-сквиз")
        elif div["divergence"]:
            lines.append("\n⚠️ Есть расхождение OI между биржами")

    # Вердикт
    lines.append("\n🎓 <b>ВЕРДИКТ</b>")
    change_24h = item.get("change24", 0) if item else 0

    if abs(change_24h) >= MAX_24H_CHANGE_FOR_LONG:
        lines.append(f"❌ Уже поздно: 24h change {change_24h:+.2f}%")
    elif kc and bg and kc["rvol"] >= SIGNAL_RVOL_KUCOIN and bg["rvol"] >= SIGNAL_RVOL_BITGET:
        lines.append("✅ Обе биржи подтверждают сигнал")
    elif kc and kc["rvol"] >= SIGNAL_RVOL_KUCOIN:
        lines.append("⚠️ KuCoin показывает импульс, Bitget не подтверждает")
    else:
        lines.append("💤 Импульса нет")

    return "\n".join(lines)


async def test_all_apis():
    results = []
    try:
        kc = await fetch_kucoin_contracts()
        results.append(f"KuCoin contracts: {'✅ ' + str(len(kc)) if kc else '❌'}")
    except Exception as e:
        results.append(f"KuCoin: ❌ {e}")

    try:
        bg = await fetch_bitget_tickers()
        results.append(f"Bitget tickers: {'✅ ' + str(len(bg)) if bg else '❌'}")
    except Exception as e:
        results.append(f"Bitget: ❌ {e}")

    oi_btc = await collect_all_oi("BTC")
    for ex, val in oi_btc.items():
        status = f"✅ {val:,.0f}" if val > 0 else "❌"
        results.append(f"OI {ex}: {status}")

    return "🧪 <b>ТЕСТ API</b>\n\n" + "\n".join(results)


# ============================================================
# TELEGRAM POLLING
# ============================================================

async def telegram_poll_loop():
    global LAST_UPDATE_ID
    if not BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    log.info("Telegram polling started")

    while True:
        try:
            params = {"timeout": 25, "offset": LAST_UPDATE_ID + 1}
            async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    await asyncio.sleep(5)
                    continue
                data = await resp.json()
                if not data.get("ok"):
                    await asyncio.sleep(5)
                    continue
                for upd in data.get("result", []):
                    LAST_UPDATE_ID = max(LAST_UPDATE_ID, upd.get("update_id", 0))
                    try:
                        await handle_telegram_update(upd)
                    except Exception as e:
                        STATS["update_errors"] += 1
                        log.exception("handle error: %s", e)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("Telegram poll error: %s", e)
            await asyncio.sleep(5)


async def handle_telegram_update(upd):
    msg = upd.get("message")
    if not msg:
        return
    chat_id = msg.get("chat", {}).get("id")
    text = str(msg.get("text", "")).strip()

    if CHAT_ID and str(chat_id) != str(CHAT_ID):
        return

    parts = text.split()
    cmd = parts[0].lower() if parts else ""

    if cmd in ("/start", "/help"):
        await send_telegram(
            "🛠 <b>Команды v7.4 (OI-FIRST)</b>\n\n"
            "/check SYMBOL — полная сверка монеты\n"
            "/test — проверка всех API\n"
            "/stats — статистика\n"
            "/wl — текущий watchlist\n\n"
            "Пример: <code>/check MYX</code>"
        )
        return

    if cmd == "/check":
        if len(parts) < 2:
            await send_telegram("Использование: <code>/check MYX</code>")
            return
        symbol = norm(parts[1])
        await send_telegram(f"🔎 Проверяю <b>{symbol}</b>...")
        try:
            report = await full_market_report(symbol)
            STATS["manual_checks"] += 1
            await send_telegram(report)
        except Exception as e:
            await send_telegram(f"❌ Ошибка: <code>{e}</code>")
        return

    if cmd == "/test":
        await send_telegram("🧪 Тестирую API...")
        result = await test_all_apis()
        await send_telegram(result)
        return

    if cmd == "/stats":
        await send_telegram(
            f"📊 <b>Статистика v7.4</b>\n\n"
            f"Юниверс: {len(UNIVERSE)}\n"
            f"Watchlist: {len(WATCHLIST)}\n"
            f"OI-сканов: {STATS['oi_scans']}\n"
            f"OI-аномалий: {STATS['oi_anomalies_found']}\n\n"
            f"<b>Сигналов:</b> {STATS['confirmed_signals']}\n"
            f"Усиленных: {STATS['amplified_signals']}\n\n"
            f"<b>Отсевы:</b>\n"
            f"24h change: {STATS['rejected_24h_change']}\n"
            f"Late entry: {STATS['rejected_late_entry']}\n"
            f"Distribution: {STATS['rejected_distribution']}\n"
            f"Нет RVOL KuCoin: {STATS['rejected_no_rvol_kc']}\n"
            f"Нет RVOL Bitget: {STATS['rejected_no_rvol_bg']}\n"
            f"OI падает везде: {STATS['rejected_oi_all_negative']}\n\n"
            f"<b>HTTP:</b> KC {STATS['kucoin_requests']} | BG {STATS['bitget_requests']} | "
            f"BB {STATS['bybit_requests']} | OKX {STATS['okx_requests']} | "
            f"Gate {STATS['gate_requests']} | BX {STATS['bingx_requests']}"
        )
        return

    if cmd == "/wl":
        if not WATCHLIST:
            await send_telegram("Watchlist пуст")
            return
        now = time.time()
        lines = ["🎯 <b>Watchlist (OI-аномалии)</b>\n"]
        for b, w in WATCHLIST.items():
            age_min = int((now - w["added_at"]) / 60)
            oi_d = w.get("last_oi_delta", w.get("oi_delta", 0))
            lines.append(f"  • <code>{b}</code> | ΔOI +{oi_d:.2f}% | {age_min}м")
        await send_telegram("\n".join(lines))
        return

    if cmd.startswith("/"):
        await send_telegram(f"❓ Неизвестная команда: <code>{cmd}</code>\nПопробуй /help")


# ============================================================
# WEB
# ============================================================

async def index(request):
    return web.Response(
        text=f"CEX AGGREGATOR v7.4 (OI-FIRST) | Universe: {len(UNIVERSE)} | "
             f"Watchlist: {len(WATCHLIST)} | Signals: {STATS['confirmed_signals']} | "
             f"OI anomalies: {STATS['oi_anomalies_found']}",
        content_type="text/plain"
    )


async def check_endpoint(request):
    base = request.match_info.get("base", "").upper()
    if not base:
        return web.Response(text="Usage: /check/MYX")
    try:
        report = await full_market_report(base)
        STATS["manual_checks"] += 1
        plain = report.replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", "")
        return web.Response(text=plain, content_type="text/plain")
    except Exception as e:
        return web.Response(text=f"Error: {e}")


async def health(request):
    return web.Response(text="ok")


async def start_background(app):
    global SESSION
    SESSION = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=50, ttl_dns_cache=300))
    app["scanner_task"] = asyncio.create_task(scanner_loop())
    app["telegram_task"] = asyncio.create_task(telegram_poll_loop())
    await send_telegram(
        "🚀 <b>CEX AGGREGATOR v7.4 (OI-FIRST)</b>\n\n"
        "🎯 Логика:\n"
        "1. Скан OI KuCoin каждые 20 сек\n"
        "2. OI-аномалия → монета на карандаш\n"
        "3. Агрегат OI по 6 биржам\n"
        "4. RVOL + движение как подтверждение\n\n"
        "🛡 Фильтры: 24h change, 30m move, distribution\n"
        "💬 /check /test /stats /wl"
    )


async def cleanup(app):
    for key in ("scanner_task", "telegram_task"):
        t = app.get(key)
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
app.router.add_get("/health", health)
app.router.add_get("/check/{base}", check_endpoint)
app.on_startup.append(start_background)
app.on_cleanup.append(cleanup)


if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
