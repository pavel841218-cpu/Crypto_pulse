import asyncio
import aiohttp
from aiohttp import web
import os
import time
import logging
from collections import defaultdict, deque

# ============================================================
# CEX AGGREGATOR v7.2 (FOCUSED MONITORING + QUIET INFLOW + TELEGRAM COMMANDS)
# 6 бирж: KuCoin, Bitget, BingX, OKX, Gate.io, Bybit (БЕЗ Binance)
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
WATCHLIST_TTL_SEC = 12 * 3600
WATCHLIST_MAX_SIZE = 60

ANOMALY_RVOL_THRESHOLD = 1.8
ANOMALY_MIN_MOVE_PCT = 0.3

SIGNAL_RVOL_KUCOIN = 2.0
SIGNAL_RVOL_BITGET = 1.5
SIGNAL_MIN_MOVE_KUCOIN = 0.4
SIGNAL_MIN_MOVE_BITGET = 0.3

OI_DIVERGENCE_MIN_PCT = 0.5
MIN_POSITIVE_OI_SOURCES = 1
MIN_OI_SOURCES = 2
MIN_CANDLES = 20

QUIET_INFLOW_ENABLED = True
OI_SHORT_LOOKBACK = 15
OI_QUIET_LOOKBACK = 40
QUIET_MAX_PRICE_MOVE_PCT = 1.8
QUIET_MIN_KUCOIN_OI_PCT = 5.0
QUIET_MIN_LEAD_OVER_OTHERS = 4.0
QUIET_COOLDOWN_SEC = 3 * 3600

# Telegram commands
TELEGRAM_POLL_TIMEOUT = 25

KUCOIN_BASE = "https://api-futures.kucoin.com"
BITGET_BASE = "https://api.bitget.com"
BYBIT_BASE = "https://api.bybit.com"
OKX_BASE = "https://www.okx.com"
GATE_BASE = "https://api.gateio.ws"
BINGX_BASE = "https://open-api.bingx.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("CEX-AGGREGATOR-v7.2")

SESSION = None
UNIVERSE = {}
WATCHLIST = {}
LAST_SIGNAL = {}
LAST_QUIET_SIGNAL = {}
LAST_UPDATE_ID = 0

OI_SNAPSHOTS = defaultdict(lambda: deque(maxlen=90))

STATS = {
    "kucoin_requests": 0, "bitget_requests": 0, "bybit_requests": 0,
    "okx_requests": 0, "gate_requests": 0, "bingx_requests": 0,
    "scans": 0, "watchlist_added": 0, "watchlist_expired": 0,
    "confirmed_signals": 0, "amplified_signals": 0, "quiet_signals": 0,
    "manual_checks": 0,
    "rejected_no_move": 0, "rejected_no_rvol": 0,
    "rejected_no_confirmation": 0, "rejected_oi_all_negative": 0,
    "rejected_oi_weak": 0,
}


# ============================================================
# HTTP HELPER & UTILS
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
# CEX FETCHERS (6 бирж)
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


async def fetch_kucoin_candles(symbol):
    url = f"{KUCOIN_BASE}/api/v1/kline/query"
    data = await http_get(url, params={"symbol": symbol, "granularity": "5"}, service="kucoin")
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


async def fetch_kucoin_oi(symbol):
    url = f"{KUCOIN_BASE}/api/v1/contracts/{symbol}"
    data = await http_get(url, service="kucoin")
    if not data or not isinstance(data.get("data"), dict):
        return 0.0
    return num(data["data"].get("openInterest"))


async def fetch_kucoin_ticker_oi(symbol):
    """Альтернативный источник OI KuCoin (ticker)."""
    url = f"{KUCOIN_BASE}/api/v1/ticker"
    data = await http_get(url, params={"symbol": symbol}, service="kucoin")
    if not data or not isinstance(data.get("data"), dict):
        return 0.0
    return num(data["data"].get("openInterest"))


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
# METRICS & ANALYTICS
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


async def collect_all_oi(base):
    item = UNIVERSE.get(base)
    kc_sym = item["kucoin_symbol"] if item else f"{base}USDTM"

    kc, bg, bx, okx, gate, bb = await asyncio.gather(
        fetch_kucoin_ticker_oi(kc_sym),
        fetch_bitget_oi(f"{base}USDT"),
        fetch_bingx_oi(base),
        fetch_okx_oi(base),
        fetch_gate_oi(base),
        fetch_bybit_oi(base),
    )
    return {"kucoin": kc, "bitget": bg, "bingx": bx, "okx": okx, "gate": gate, "bybit": bb}


def calculate_oi_deltas(base, current_snapshot, lookback=OI_SHORT_LOOKBACK):
    now = time.time()
    snapshots = OI_SNAPSHOTS[base]
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


def detect_quiet_inflow(base, current_oi, price_move_pct):
    if not QUIET_INFLOW_ENABLED:
        return {"is_quiet": False}

    deltas = calculate_oi_deltas(base, current_oi, lookback=OI_QUIET_LOOKBACK)
    kc_delta = deltas.get("kucoin", 0.0)

    others = [deltas.get(ex, 0.0) for ex in ["bitget", "bingx", "bybit", "okx", "gate"]
              if abs(deltas.get(ex, 0.0)) > 0.01]
    avg_others = sum(others) / len(others) if others else 0.0
    lead = kc_delta - avg_others

    is_quiet = (
        abs(price_move_pct) <= QUIET_MAX_PRICE_MOVE_PCT
        and kc_delta >= QUIET_MIN_KUCOIN_OI_PCT
        and lead >= QUIET_MIN_LEAD_OVER_OTHERS
    )

    return {
        "is_quiet": is_quiet,
        "kucoin_oi_delta": round(kc_delta, 2),
        "avg_others": round(avg_others, 2),
        "lead": round(lead, 2),
        "price_move": round(price_move_pct, 2),
    }


# ============================================================
# WATCHLIST
# ============================================================

def add_to_watchlist(base, source, metrics):
    now = time.time()
    if base in WATCHLIST:
        WATCHLIST[base]["expires_at"] = now + WATCHLIST_TTL_SEC
        return False
    if len(WATCHLIST) >= WATCHLIST_MAX_SIZE:
        oldest = min(WATCHLIST.items(), key=lambda x: x[1]["added_at"])[0]
        WATCHLIST.pop(oldest, None)
    WATCHLIST[base] = {
        "added_at": now, "expires_at": now + WATCHLIST_TTL_SEC,
        "source": source, "initial_metrics": metrics,
    }
    STATS["watchlist_added"] += 1
    log.info("🎯 Кандидат в фокус: %s | Источник: %s | RVOL: %.2fx", base, source, metrics["rvol"])
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


async def send_quiet_signal(base, kc, quiet, watchlist_info):
    hold_min = int((time.time() - watchlist_info.get("added_at", time.time())) / 60)
    msg = (
        f"🟡 <b>ТИХОЕ ВЛИТИЕ: {base}USDT</b>\n"
        f"Подготовка к возможному пампу\n\n"
        f"📌 В фокусе: {hold_min} мин\n\n"
        f"📊 Изменение цены: <b>{quiet['price_move']:+.2f}%</b>\n"
        f"📈 KuCoin OI: <b>+{quiet['kucoin_oi_delta']}%</b>\n"
        f"📉 Остальные биржи: <b>{quiet['avg_others']:+.2f}%</b>\n"
        f"⚡ Лид KuCoin: <b>+{quiet['lead']}%</b>\n\n"
        f"🎯 KuCoin RVOL: {kc['rvol']:.2f}x"
    )
    await send_telegram(msg)
    STATS["quiet_signals"] += 1


async def send_signal(base, kc, bg, div, watchlist_info):
    source = watchlist_info.get("source", "?")
    hold_min = int((time.time() - watchlist_info.get("added_at", time.time())) / 60)

    if div["amplifier"]:
        signal_type = "🔥🔥🔥 МАКСИМАЛЬНЫЙ СИГНАЛ (Divergence)"
    elif div["divergence"]:
        signal_type = "🔥 УСИЛЕННЫЙ СИГНАЛ (Дисбаланс OI)"
    else:
        signal_type = "✅ СТАНДАРТНЫЙ СИГНАЛ (Рост OI)"

    pos = ", ".join([f"{k.upper()}: +{v:.2f}%" for k, v in div["positive"].items()]) or "нет"
    neg = ", ".join([f"{k.upper()}: {v:.2f}%" for k, v in div["negative"].items()]) or "нет"

    msg = (
        f"🚀 <b>СИГНАЛ: {base}USDT</b>\n"
        f"{signal_type}\n\n"
        f"📌 Источник: {source.upper()} | В отслеживании: {hold_min} мин\n\n"
        f"🎯 KuCoin: Move +{kc['move_pct']:.2f}% | RVOL <b>{kc['rvol']:.2f}x</b>\n"
        f"✅ Bitget: Move +{bg['move_pct']:.2f}% | RVOL <b>{bg['rvol']:.2f}x</b>\n\n"
        f"📈 Приток OI: <code>{pos}</code>\n"
        f"🔻 Сброс OI: <code>{neg}</code>"
    )
    await send_telegram(msg)


# ============================================================
# FULL MARKET REPORT — ручная проверка монеты
# ============================================================

async def full_market_report(base):
    """
    Полная проверка монеты по всем 6 биржам.
    Возвращает отформатированный текст для Telegram.
    """
    base = norm(base)

    # Свечи KuCoin и Bitget (для метрик)
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

    # KuCoin
    if kc:
        lines.append(
            f"🎯 <b>KuCoin (лидер)</b>\n"
            f"  Цена: <code>{kc['close']:.8g}</code>\n"
            f"  Move 5m: <b>+{kc['move_pct']:.2f}%</b>\n"
            f"  RVOL: <b>{kc['rvol']:.2f}x</b>\n"
            f"  Объём: ${kc['volume_usd']:,.0f}"
        )
    else:
        lines.append("🎯 <b>KuCoin</b>: ❌ нет данных (мало свечей или символ не найден)")

    # Bitget
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

    # OI по всем биржам
    lines.append("\n📊 <b>OI по всем 6 биржам</b>")
    exchange_labels = {
        "kucoin": "KuCoin",
        "bitget": "Bitget",
        "bingx": "BingX",
        "okx": "OKX",
        "gate": "Gate.io",
        "bybit": "Bybit",
    }
    for ex, val in oi_all.items():
        label = exchange_labels.get(ex, ex)
        if val > 0:
            lines.append(f"  • {label}: <code>{val:,.0f}</code>")
        else:
            lines.append(f"  • {label}: ❌ нет")

    # Дельты OI
    deltas = calculate_oi_deltas(base, oi_all, lookback=OI_SHORT_LOOKBACK)
    has_deltas = any(abs(v) > 0.01 for v in deltas.values())
    if has_deltas:
        lines.append("\n📈 <b>ΔOI за последние ~5 мин</b>")
        for ex, d in deltas.items():
            label = exchange_labels.get(ex, ex)
            sign = "🟢" if d > 0 else ("🔴" if d < 0 else "⚪")
            lines.append(f"  • {label}: {sign} <b>{d:+.2f}%</b>")

    # Дивергенция
    div = analyze_oi_divergence(deltas)
    if div["amplifier"]:
        lines.append("\n🔥 <b>УСИЛИТЕЛЬ:</b> сильное расхождение OI → шорт-сквиз!")
    elif div["divergence"]:
        lines.append("\n⚠️ <b>Дивергенция OI</b> (есть расхождение между биржами)")

    # Вердикт
    lines.append("\n🎓 <b>ВЕРДИКТ</b>")

    if not kc and not bg:
        lines.append("❌ Нет данных ни с KuCoin, ни с Bitget")
    elif kc and kc["rvol"] >= SIGNAL_RVOL_KUCOIN and kc["move_pct"] >= SIGNAL_MIN_MOVE_KUCOIN:
        if bg and bg["rvol"] >= SIGNAL_RVOL_BITGET:
            lines.append("✅ <b>ОБЕ БИРЖИ ПОДТВЕРЖДАЮТ</b> — это сигнал")
            if div["positive"]:
                lines.append(f"📈 OI растёт: {', '.join(div['positive'].keys())}")
            if div["negative"]:
                lines.append(f"📉 OI падает: {', '.join(div['negative'].keys())}")
        else:
            lines.append("⚠️ KuCoin показывает импульс, но Bitget не подтверждает")
    elif bg and bg["rvol"] >= SIGNAL_RVOL_BITGET and bg["move_pct"] >= SIGNAL_MIN_MOVE_BITGET:
        lines.append("⚠️ Импульс на Bitget, но KuCoin не подтверждает")
    else:
        lines.append("💤 Сигнала нет — нет RVOL/движения на обеих биржах")

    return "\n".join(lines)


async def test_all_apis():
    """Проверка всех API — какие работают, какие нет."""
    results = []

    # KuCoin
    try:
        kc = await fetch_kucoin_contracts()
        results.append(f"KuCoin contracts: {'✅ ' + str(len(kc)) if kc else '❌'}")
    except Exception as e:
        results.append(f"KuCoin: ❌ {e}")

    # Bitget
    try:
        bg = await fetch_bitget_tickers()
        results.append(f"Bitget tickers: {'✅ ' + str(len(bg)) if bg else '❌'}")
    except Exception as e:
        results.append(f"Bitget: ❌ {e}")

    # OI по BTC на всех биржах
    oi_btc = await collect_all_oi("BTC")
    for ex, val in oi_btc.items():
        status = f"✅ {val:,.0f}" if val > 0 else "❌"
        results.append(f"OI {ex}: {status}")

    return "🧪 <b>ТЕСТ ВСЕХ API</b>\n\n" + "\n".join(results)


# ============================================================
# TELEGRAM POLLING (команды)
# ============================================================

async def telegram_poll_loop():
    global LAST_UPDATE_ID
    if not BOT_TOKEN:
        log.warning("BOT_TOKEN не задан — команды недоступны")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    log.info("Telegram polling started")

    while True:
        try:
            params = {
                "timeout": TELEGRAM_POLL_TIMEOUT,
                "offset": LAST_UPDATE_ID + 1,
            }
            async with SESSION.get(
                url, params=params,
                timeout=aiohttp.ClientTimeout(total=TELEGRAM_POLL_TIMEOUT + 5)
            ) as resp:
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
                        log.exception("handle update error: %s", e)
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

    # Фильтр по CHAT_ID (безопасность)
    if CHAT_ID and str(chat_id) != str(CHAT_ID):
        log.info("Ignored message from chat %s (expected %s)", chat_id, CHAT_ID)
        return

    log.info("Telegram command: %s", text)

    parts = text.split()
    cmd = parts[0].lower() if parts else ""

    if cmd in ("/start", "/help"):
        await send_telegram(
            "🛠 <b>Команды бота v7.2</b>\n\n"
            "/check SYMBOL — полная сверка монеты по всем 6 биржам\n"
            "/test — проверка всех API (какие живы)\n"
            "/stats — статистика сканера\n"
            "/wl — текущий watchlist\n"
            "/stats — сводка\n\n"
            "Примеры:\n"
            "<code>/check MYX</code>\n"
            "<code>/check BTC</code>\n"
            "<code>/check BR</code>"
        )
        return

    if cmd == "/check":
        if len(parts) < 2:
            await send_telegram("Использование: <code>/check SYMBOL</code> (например, <code>/check MYX</code>)")
            return
        symbol = norm(parts[1])
        await send_telegram(f"🔎 Проверяю <b>{symbol}</b> по всем 6 биржам...")
        try:
            report = await full_market_report(symbol)
            STATS["manual_checks"] += 1
            await send_telegram(report)
        except Exception as e:
            await send_telegram(f"❌ Ошибка проверки: <code>{e}</code>")
        return

    if cmd == "/test":
        await send_telegram("🧪 Тестирую все API, подожди 5-10 секунд...")
        try:
            result = await test_all_apis()
            await send_telegram(result)
        except Exception as e:
            await send_telegram(f"❌ Ошибка теста: <code>{e}</code>")
        return

    if cmd == "/stats":
        await send_telegram(
            f"📊 <b>Статистика v7.2</b>\n\n"
            f"Юниверс: {len(UNIVERSE)}\n"
            f"Watchlist: {len(WATCHLIST)} / {WATCHLIST_MAX_SIZE}\n"
            f"Сканов: {STATS['scans']}\n\n"
            f"Сигналов: <b>{STATS['confirmed_signals']}</b>\n"
            f"Усиленных: {STATS['amplified_signals']}\n"
            f"Тихих влитий: {STATS['quiet_signals']}\n"
            f"Ручных проверок: {STATS['manual_checks']}\n\n"
            f"<b>HTTP</b>\n"
            f"KC {STATS['kucoin_requests']} | BG {STATS['bitget_requests']} | "
            f"BB {STATS['bybit_requests']} | OKX {STATS['okx_requests']} | "
            f"Gate {STATS['gate_requests']} | BX {STATS['bingx_requests']}"
        )
        return

    if cmd == "/wl":
        if not WATCHLIST:
            await send_telegram("Watchlist пуст")
            return
        now = time.time()
        lines = ["🎯 <b>Watchlist</b>\n"]
        for b, w in WATCHLIST.items():
            age_min = int((now - w["added_at"]) / 60)
            left_min = int((w["expires_at"] - now) / 60)
            src = w.get("source", "?")
            lines.append(f"  • <code>{b}</code> | src={src} | {age_min}м (осталось {left_min}м)")
        await send_telegram("\n".join(lines))
        return

    # Неизвестная команда
    if cmd.startswith("/"):
        await send_telegram(f"❓ Неизвестная команда: <code>{cmd}</code>\n\nПопробуй /help")


# ============================================================
# FOCUSED LOGIC
# ============================================================

async def focus_candidate_analysis(base):
    item = UNIVERSE.get(base)
    if not item:
        return

    kc_candles, bg_candles = await asyncio.gather(
        fetch_kucoin_candles(item["kucoin_symbol"]),
        fetch_bitget_candles(item["bitget_symbol"]),
    )
    kc = calc_metrics(kc_candles)
    bg = calc_metrics(bg_candles)
    if not kc or not bg:
        return

    oi_snapshot = await collect_all_oi(base)

    # 1. Тихое влитие
    quiet = detect_quiet_inflow(base, oi_snapshot, kc["move_pct"])
    if quiet["is_quiet"]:
        if time.time() - LAST_QUIET_SIGNAL.get(base, 0) > QUIET_COOLDOWN_SEC:
            LAST_QUIET_SIGNAL[base] = time.time()
            await send_quiet_signal(base, kc, quiet, WATCHLIST.get(base, {}))

    # 2. Боевые условия
    if kc["rvol"] < SIGNAL_RVOL_KUCOIN:
        STATS["rejected_no_rvol"] += 1
        return
    if kc["move_pct"] < SIGNAL_MIN_MOVE_KUCOIN:
        STATS["rejected_no_move"] += 1
        return
    if bg["rvol"] < SIGNAL_RVOL_BITGET or bg["move_pct"] < SIGNAL_MIN_MOVE_BITGET:
        STATS["rejected_no_confirmation"] += 1
        return

    deltas = calculate_oi_deltas(base, oi_snapshot, lookback=OI_SHORT_LOOKBACK)
    div = analyze_oi_divergence(deltas)

    if div["sources"] == 0:
        return
    if not div["positive"] and div["negative"]:
        STATS["rejected_oi_all_negative"] += 1
        return
    if len(div["positive"]) < MIN_POSITIVE_OI_SOURCES:
        STATS["rejected_oi_weak"] += 1
        return
    if time.time() - LAST_SIGNAL.get(base, 0) < SIGNAL_COOLDOWN_SEC:
        return

    LAST_SIGNAL[base] = time.time()
    STATS["confirmed_signals"] += 1
    if div["amplifier"]:
        STATS["amplified_signals"] += 1

    await send_signal(base, kc, bg, div, WATCHLIST.get(base, {}))
    WATCHLIST.pop(base, None)


# ============================================================
# SCANNER LOOPS
# ============================================================

async def refresh_universe():
    global UNIVERSE
    kc = await fetch_kucoin_contracts()
    bitget = await fetch_bitget_tickers()
    if not kc or not bitget:
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
        }

    sorted_u = sorted(universe.items(), key=lambda x: x[1]["volume24"], reverse=True)
    UNIVERSE = dict(sorted_u[:MAX_UNIVERSE_SYMBOLS])
    log.info("🌐 Юниверс: %d пар (KuCoin ∩ Bitget)", len(UNIVERSE))


async def scan_cycle():
    if not UNIVERSE:
        return
    STATS["scans"] += 1
    candidates = list(UNIVERSE.keys())[:MAX_SCAN_CANDIDATES]
    semaphore = asyncio.Semaphore(8)

    async def scan_one(base):
        async with semaphore:
            if base in WATCHLIST:
                return
            item = UNIVERSE.get(base)
            if not item:
                return
            kc_c, bg_c = await asyncio.gather(
                fetch_kucoin_candles(item["kucoin_symbol"]),
                fetch_bitget_candles(item["bitget_symbol"]),
            )
            kc, bg = calc_metrics(kc_c), calc_metrics(bg_c)
            if kc and kc["rvol"] >= ANOMALY_RVOL_THRESHOLD and kc["move_pct"] >= ANOMALY_MIN_MOVE_PCT:
                add_to_watchlist(base, "kucoin", kc)
            elif bg and bg["rvol"] >= ANOMALY_RVOL_THRESHOLD and bg["move_pct"] >= ANOMALY_MIN_MOVE_PCT:
                add_to_watchlist(base, "bitget", bg)

    await asyncio.gather(*[scan_one(b) for b in candidates])

    if WATCHLIST:
        active = list(WATCHLIST.keys())
        log.info("🎯 Фокус: %d кандидатов", len(active))
        await asyncio.gather(*[focus_candidate_analysis(b) for b in active])

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
# WEB SERVER
# ============================================================

async def index(request):
    return web.Response(
        text=f"CEX AGGREGATOR v7.2 | Universe: {len(UNIVERSE)} | Watchlist: {len(WATCHLIST)} | "
             f"Signals: {STATS['confirmed_signals']} | Quiet: {STATS['quiet_signals']}",
        content_type="text/plain"
    )


async def check_endpoint(request):
    """HTTP endpoint для ручной проверки: /check/MYX"""
    base = request.match_info.get("base", "").upper()
    if not base:
        return web.Response(text="Usage: /check/MYX", content_type="text/plain")
    try:
        report = await full_market_report(base)
        STATS["manual_checks"] += 1
        # Убираем HTML-теги для plain-text ответа
        plain = report.replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", "")
        return web.Response(text=plain, content_type="text/plain")
    except Exception as e:
        return web.Response(text=f"Error: {e}", content_type="text/plain")


async def health(request):
    return web.Response(text="ok")


async def start_background(app):
    global SESSION
    SESSION = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=50, ttl_dns_cache=300))

    # Фоновые задачи
    app["scanner_task"] = asyncio.create_task(scanner_loop())
    app["telegram_task"] = asyncio.create_task(telegram_poll_loop())

    await send_telegram(
        "🚀 <b>CEX AGGREGATOR v7.2 Запущен!</b>\n\n"
        "Мониторинг 6 CEX (без Binance)\n"
        "🎯 Детектор тихого влития\n"
        "📊 Динамическая фокусировка\n"
        "💬 Команды: /check /test /stats /wl"
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
