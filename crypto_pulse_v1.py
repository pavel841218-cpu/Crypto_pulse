"""
PUMP-SCREENER v1.0 — OKX 5m Real-Time Pattern Scanner

- WS-first: candle5m stream, один сокет, ~150 подписок
- REST только для: instruments (1 раз), tickers (refresh 5 мин)
- Две стратегии: direct breakout / dry-sweep → expansion
- Live tracking после входа
- Bandwidth: ~3-5 GB/month
"""

import asyncio
import aiohttp
from aiohttp import web, WSMsgType
import os
import time
import gc
import json
import logging
from html import escape
from collections import defaultdict, deque
from typing import Optional

# ═══════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID   = os.environ.get("CHAT_ID", "")
PORT      = int(os.environ.get("PORT", "10000"))

# ─── Timeframes ───
BAR                = "5m"
BAR_SECONDS        = 300
SCAN_INTERVAL_SEC  = int(os.environ.get("SCAN_INTERVAL_SEC", "15"))
UNIVERSE_REFRESH   = int(os.environ.get("UNIVERSE_REFRESH_SEC", "300"))
SIGNAL_COOLDOWN    = int(os.environ.get("SIGNAL_COOLDOWN_SEC", "1800"))
CANDLE_BUFFER_SIZE = 30

# ─── Universe filter ───
MIN_VOL_24H   = float(os.environ.get("MIN_24H_VOLUME_USDT", "200000"))
MIN_PRICE     = float(os.environ.get("MIN_PRICE_USDT", "0.000001"))
MAX_PRICE     = float(os.environ.get("MAX_PRICE_USDT", "50.0"))
MAX_UNIVERSE  = int(os.environ.get("MAX_UNIVERSE_SYMBOLS", "150"))
MIN_CHANGE_24H = float(os.environ.get("MIN_CHANGE_24H_PCT", "-50.0"))
MAX_CHANGE_24H = float(os.environ.get("MAX_CHANGE_24H_PCT", "200.0"))

# ─── Shelf (полка) ───
SH_MIN_CANDLES      = int(os.environ.get("SHELF_MIN_CANDLES", "10"))
SH_MAX_CANDLES      = int(os.environ.get("SHELF_MAX_CANDLES", "20"))
SH_RANGE_MIN_PCT    = float(os.environ.get("SHELF_RANGE_MIN_PCT", "0.30"))
SH_RANGE_MAX_PCT    = float(os.environ.get("SHELF_RANGE_MAX_PCT", "3.50"))
SH_MAX_NET_MOVE     = float(os.environ.get("SHELF_MAX_NET_MOVE_PCT", "2.00"))
SH_MAX_DRIFT        = float(os.environ.get("SHELF_MAX_DRIFT_PCT", "1.50"))
SH_MIN_INSIDE       = float(os.environ.get("SHELF_MIN_INSIDE_RATIO", "0.75"))
SH_MAX_DIST_HIGH    = float(os.environ.get("SHELF_MAX_DIST_FROM_HIGH_PCT", "1.20"))
SH_VOL_STABILITY    = float(os.environ.get("SHELF_VOL_STABILITY_MAX", "2.5"))
SH_MIN_AVG_VOL_USDT = float(os.environ.get("SHELF_MIN_AVG_VOL_USDT", "2000"))
SH_MAX_AGE_SEC      = int(os.environ.get("SHELF_MAX_AGE_SEC", str(6 * 3600)))

# ─── Branch A: direct breakout ───
A_MIN_BREAKOUT_PCT = float(os.environ.get("DIRECT_BREAKOUT_MIN_PCT", "0.35"))
A_MIN_RVOL         = float(os.environ.get("DIRECT_BREAKOUT_MIN_RVOL", "1.8"))
A_MIN_VOL_USDT     = float(os.environ.get("DIRECT_BREAKOUT_MIN_VOL_USDT", "3000"))
A_MIN_PCT_CHANGE   = float(os.environ.get("DIRECT_BREAKOUT_MIN_PCT_CHANGE", "1.5"))

# ─── Branch B: sweep → expansion ───
B_SWEEP_LOOKBACK       = int(os.environ.get("SWEEP_LOOKBACK", "6"))
B_SWEEP_MIN_BREAK_PCT  = float(os.environ.get("SWEEP_BREAKDOWN_MIN_PCT", "0.10"))
B_SWEEP_MAX_VOL_RATIO  = float(os.environ.get("SWEEP_MAX_VOL_RATIO", "0.45"))
B_SWEEP_MUST_CLOSE_IN  = os.environ.get("SWEEP_MUST_CLOSE_INSIDE", "1") == "1"
B_SWEEP_MAX_AGE        = int(os.environ.get("SWEEP_MAX_AGE_CANDLES", "5"))
B_EXP_MIN_VS_SWEEP     = float(os.environ.get("EXPANSION_MIN_RATIO_VS_SWEEP", "3.0"))
B_EXP_MIN_VS_PREV      = float(os.environ.get("EXPANSION_MIN_VOL_VS_PREV", "3.0"))
B_EXP_MIN_VS_AVG       = float(os.environ.get("EXPANSION_MIN_VOL_VS_AVG", "0.6"))
B_EARLY_ENABLED        = os.environ.get("EARLY_SIGNAL_ENABLED", "1") == "1"
B_EARLY_MIN_VS_PREV    = float(os.environ.get("EARLY_MIN_VOL_VS_PREV", "8.0"))

# ─── Live candle gates ───
LIVE_MIN_ELAPSED_SEC = int(os.environ.get("LIVE_MIN_ELAPSED_SEC", "60"))
LIVE_MIN_VOL_USDT    = float(os.environ.get("LIVE_MIN_CURRENT_VOL_USDT", "2000"))

# ─── Track (post-signal) ───
TRK_TIMEOUT_SEC     = int(os.environ.get("TRACK_TIMEOUT_SEC", str(6 * 3600)))
TRK_HOLD_TOL_PCT    = float(os.environ.get("TRACK_HOLD_LEVEL_TOLERANCE_PCT", "0.10"))
TRK_FAKEOUT_PCT     = float(os.environ.get("TRACK_FAKEOUT_BACK_INSIDE_PCT", "0.30"))
TRK_PROFIT_ALERT    = float(os.environ.get("TRACK_PROFIT_ALERT_PCT", "2.5"))
TRK_INVALIDATE_PCT  = float(os.environ.get("TRACK_DRAWDOWN_INVALIDATE_PCT", "-2.5"))

# ─── Climax ───
CLX_MIN_RVOL      = float(os.environ.get("CLIMAX_RVOL_THRESHOLD", "3.0"))
CLX_MIN_WICK      = float(os.environ.get("CLIMAX_MIN_WICK_RATIO", "0.40"))
CLX_MIN_PROFIT    = float(os.environ.get("CLIMAX_MIN_PROFIT_PCT", "3.0"))

# ─── OKX ───
OKX_REST = "https://www.okx.com"
OKX_WS   = "wss://ws.okx.com:8443/ws/v5/public"

# ═══════════════════════════════════════════════════════════════
#  LOGGING & GLOBALS
# ═══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("SCREENER")

SESSION: Optional[aiohttp.ClientSession] = None
HTTP_SEM: Optional[asyncio.Semaphore] = None
START_TS = time.time()

# ─── State ───
UNIVERSE: dict[str, dict] = {}                        # base -> {symbol, price, vol24, change24}
CANDLES: dict[str, deque] = defaultdict(lambda: deque(maxlen=CANDLE_BUFFER_SIZE))
SHELVES: dict[str, dict] = {}                         # base -> shelf
TRACKS: dict[str, dict] = {}                          # base -> track
LAST_SIGNAL: dict[str, float] = {}
CT_VAL: dict[str, float] = {}

# ─── WS ───
WS_TASK: Optional[asyncio.Task] = None
WS_SUBSCRIBED: set[str] = set()
WS_DESIRED: set[str] = set()

# ─── Stats ───
STATS = defaultdict(int)

# ═══════════════════════════════════════════════════════════════
#  UTILS
# ═══════════════════════════════════════════════════════════════

def num(v, default=0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def base_of(inst_id: str) -> str:
    """BTC-USDT-SWAP → BTC"""
    if not inst_id:
        return ""
    s = inst_id.upper()
    if s.endswith("-USDT-SWAP"):
        return s[:-10]
    if s.endswith("-USDT"):
        return s[:-5]
    return s


async def http_get(url, params=None, timeout=8, retries=1):
    if not SESSION or SESSION.closed:
        return None
    for attempt in range(retries + 1):
        try:
            async with HTTP_SEM:
                async with SESSION.get(
                    url, params=params,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as r:
                    if r.status == 429:
                        STATS["http_429"] += 1
                        if attempt < retries:
                            await asyncio.sleep(0.8 * (attempt + 1))
                            continue
                        return None
                    if r.status >= 400:
                        return None
                    return await r.json(content_type=None)
        except (asyncio.TimeoutError, aiohttp.ClientError):
            if attempt < retries:
                await asyncio.sleep(0.3)
        except Exception:
            break
    return None

# ═══════════════════════════════════════════════════════════════
#  OKX REST — только 2 вызова: instruments, tickers
# ═══════════════════════════════════════════════════════════════

async def load_instruments():
    """Один раз при старте: ctVal для всех SWAP."""
    data = await http_get(
        f"{OKX_REST}/api/v5/public/instruments",
        params={"instType": "SWAP"}, timeout=15,
    )
    if not data or data.get("code") != "0":
        log.warning("⚠️ instruments fail")
        return 0

    n = 0
    for inst in data.get("data", []):
        iid = str(inst.get("instId", "")).upper()
        if iid.endswith("-USDT-SWAP"):
            CT_VAL[iid] = num(inst.get("ctVal"), 1.0) or 1.0
            n += 1
    log.info("📐 ctVal: %d contracts", n)
    return n


async def refresh_universe():
    """Раз в UNIVERSE_REFRESH секунд: тикеры + фильтр."""
    global UNIVERSE

    data = await http_get(
        f"{OKX_REST}/api/v5/market/tickers",
        params={"instType": "SWAP"}, timeout=12,
    )
    if not data or data.get("code") != "0":
        log.warning("⚠️ tickers fail")
        return

    uni = {}
    for row in data.get("data", []):
        iid = str(row.get("instId", "")).upper()
        if not iid.endswith("-USDT-SWAP"):
            continue
        base = base_of(iid)
        if not base:
            continue

        price = num(row.get("last"))
        vol_ccy = num(row.get("volCcy24h"))
        vol_usdt = vol_ccy * price
        open24 = num(row.get("open24h"))
        change24 = ((price / open24) - 1) * 100 if open24 > 0 else 0.0

        if vol_usdt < MIN_VOL_24H:            continue
        if not (MIN_PRICE <= price <= MAX_PRICE):  continue
        if change24 < MIN_CHANGE_24H:         continue
        if change24 > MAX_CHANGE_24H:         continue

        uni[base] = {
            "symbol": iid,
            "price": price,
            "vol24": vol_usdt,
            "change24": change24,
        }

    # топ по обороту — максимум MAX_UNIVERSE штук
    sorted_uni = dict(sorted(
        uni.items(), key=lambda x: x[1]["vol24"], reverse=True
    )[:MAX_UNIVERSE])

    UNIVERSE = sorted_uni
    log.info("🌐 Universe: %d pairs", len(UNIVERSE))
    gc.collect()

# ═══════════════════════════════════════════════════════════════
#  OKX WEBSOCKET — candle5m stream
# ═══════════════════════════════════════════════════════════════

async def ws_send(ws, op: str, inst_ids: list[str]):
    """op = subscribe | unsubscribe"""
    if not inst_ids:
        return
    # OKX: макс 30 arg за раз? Разбиваем на чанки
    CHUNK = 30
    for i in range(0, len(inst_ids), CHUNK):
        chunk = inst_ids[i:i + CHUNK]
        msg = {
            "op": op,
            "args": [{"channel": f"candle{BAR}", "instId": x} for x in chunk],
        }
        try:
            await ws.send_json(msg)
        except Exception as e:
            log.warning("WS send fail: %s", e)
            return


async def ws_manager(ws):
    """Подписки/отписки по мере изменения желаемого набора."""
    try:
        while not ws.closed:
            desired = set(WS_DESIRED)
            current = set(WS_SUBSCRIBED)

            to_sub = list(desired - current)
            to_unsub = list(current - desired)

            if to_sub:
                await ws_send(ws, "subscribe", to_sub)
                WS_SUBSCRIBED.update(to_sub)
                STATS["ws_subs"] += len(to_sub)
                log.info("🔔 WS sub: +%d (total %d)", len(to_sub), len(WS_SUBSCRIBED))

            if to_unsub:
                await ws_send(ws, "unsubscribe", to_unsub)
                WS_SUBSCRIBED.difference_update(to_unsub)
                STATS["ws_unsubs"] += len(to_unsub)

            await asyncio.sleep(5)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.warning("ws_manager: %s", e)


async def ws_ping_loop(ws):
    try:
        while not ws.closed:
            await asyncio.sleep(25)
            if ws.closed:
                break
            try:
                await ws.send_str("ping")
            except Exception:
                break
    except asyncio.CancelledError:
        raise
    except Exception:
        pass


def _apply_candle(base: str, row: list):
    """row = [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]"""
    if not row or len(row) < 9:
        return
    try:
        c = {
            "ts": int(row[0]),
            "open": num(row[1]),
            "high": num(row[2]),
            "low": num(row[3]),
            "close": num(row[4]),
            "volume": num(row[7]),          # quote USDT volume
            "closed": row[8] == "1",
        }
    except (ValueError, IndexError):
        return

    if c["open"] <= 0:
        return

    buf = CANDLES[base]

    # Обновление последней свечи, либо добавление новой
    if buf and buf[-1]["ts"] == c["ts"]:
        buf[-1] = c
    else:
        buf.append(c)
        STATS["candles_closed"] += 1


async def websocket_loop():
    global WS_SUBSCRIBED

    backoff = 1
    while True:
        try:
            log.info("🔌 WS connecting …")
            async with SESSION.ws_connect(
                OKX_WS,
                heartbeat=None,
                autoping=True,
                receive_timeout=45,
                max_msg_size=2 * 1024 * 1024,
            ) as ws:

                WS_SUBSCRIBED = set()
                STATS["ws_reconnects"] += 1
                log.info("✅ WS connected")

                ping_task = asyncio.create_task(ws_ping_loop(ws))
                mgr_task = asyncio.create_task(ws_manager(ws))

                try:
                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            raw = msg.data

                            if raw == "pong":
                                continue
                            if raw == "ping":
                                try: await ws.send_str("pong")
                                except Exception: pass
                                continue

                            try:
                                payload = json.loads(raw)
                            except Exception:
                                continue

                            # event-сообщения (subscribe ack, error)
                            ev = payload.get("event")
                            if ev:
                                if ev == "error":
                                    log.warning("WS err: %s", str(payload)[:200])
                                elif ev == "subscribe":
                                    STATS["ws_sub_ack"] += 1
                                continue

                            arg = payload.get("arg") or {}
                            ch = arg.get("channel", "")
                            if not ch.startswith("candle"):
                                continue

                            base = base_of(str(arg.get("instId", "")))
                            if not base:
                                continue

                            for row in payload.get("data", []):
                                _apply_candle(base, row)

                        elif msg.type in (WSMsgType.CLOSED, WSMsgType.ERROR):
                            break

                finally:
                    for t in (mgr_task, ping_task):
                        t.cancel()
                        try: await t
                        except asyncio.CancelledError: pass

            backoff = 1

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("WS error: %s (backoff %ds)", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

        await asyncio.sleep(1)


def sync_ws_subscriptions():
    """Желаемые подписки = все монеты universe, у которых есть активность."""
    global WS_DESIRED
    desired = set()
    for base, u in UNIVERSE.items():
        desired.add(u["symbol"])
    # ограничим до MAX_UNIVERSE
    WS_DESIRED = desired

# ═══════════════════════════════════════════════════════════════
#  ANALYZER
# ═══════════════════════════════════════════════════════════════

def detect_shelf(candles: list[dict]) -> Optional[dict]:
    """Свечи — только ЗАКРЫТЫЕ. Возвращает shelf dict или None."""
    if len(candles) < SH_MIN_CANDLES:
        return None

    max_n = min(SH_MAX_CANDLES, len(candles))
    best = None

    for n in range(SH_MIN_CANDLES, max_n + 1):
        cs = candles[-n:]

        high = max(c["high"] for c in cs)
        low  = min(c["low"] for c in cs)
        if low <= 0:
            continue

        width = (high - low) / low * 100
        if width < SH_RANGE_MIN_PCT or width > SH_RANGE_MAX_PCT:
            continue

        first_c = cs[0]["close"]
        last_c  = cs[-1]["close"]
        if first_c <= 0:
            continue

        net_move = abs(last_c - first_c) / first_c * 100
        if net_move > SH_MAX_NET_MOVE:
            continue

        mid = (high + low) / 2
        drift = abs(last_c - mid) / mid * 100
        if drift > SH_MAX_DRIFT:
            continue

        inside = sum(1 for c in cs if low <= c["close"] <= high) / len(cs)
        if inside < SH_MIN_INSIDE:
            continue

        dist_high = (high - last_c) / last_c * 100
        if dist_high < 0 or dist_high > SH_MAX_DIST_HIGH:
            continue

        vols = [c["volume"] for c in cs]
        avg_vol = sum(vols) / len(vols)
        if avg_vol < SH_MIN_AVG_VOL_USDT:
            continue
        vmin = min(vols)
        vmax = max(vols)
        vol_stab = vmax / vmin if vmin > 0 else 999
        if vol_stab > SH_VOL_STABILITY:
            continue

        score = (
            (SH_RANGE_MAX_PCT - width) * 2
            + inside * 2
            + (SH_MAX_NET_MOVE - net_move)
            + (SH_VOL_STABILITY - vol_stab)
        )

        cand = {
            "high": high, "low": low,
            "width_pct": round(width, 3),
            "net_move_pct": round(net_move, 3),
            "inside_ratio": round(inside, 3),
            "candles": n,
            "avg_vol": avg_vol,
            "vol_stability": round(vol_stab, 3),
            "score": score,
        }
        if best is None or cand["score"] > best["score"]:
            best = cand

    return best


def live_metrics(candles: list[dict]) -> Optional[dict]:
    """Метрики по НЕЗАКРЫТОЙ свече (последней)."""
    if len(candles) < 3:
        return None

    live = candles[-1]
    prev = candles[-2]
    base = candles[-25:-1]
    if not base or live["open"] <= 0:
        return None

    elapsed = max((time.time() * 1000 - live["ts"]) / 1000, 5)
    if elapsed < LIVE_MIN_ELAPSED_SEC:
        return None
    if live["volume"] < LIVE_MIN_VOL_USDT:
        return None

    avg_base = sum(c["volume"] for c in base) / len(base)
    if avg_base <= 0:
        return None

    projected = live["volume"] * (BAR_SECONDS / elapsed)
    rvol = projected / avg_base

    prev_vol = prev["volume"]
    vol_vs_prev = projected / prev_vol if prev_vol > 0 else 0.0

    pct_change = (live["close"] - live["open"]) / live["open"] * 100

    return {
        "price": live["close"],
        "open": live["open"],
        "high": live["high"],
        "low": live["low"],
        "volume": live["volume"],
        "prev_volume": prev_vol,
        "projected_vol": projected,
        "avg_base_vol": avg_base,
        "rvol": rvol,
        "vol_vs_prev": vol_vs_prev,
        "pct_change": pct_change,
        "elapsed": elapsed,
    }


def check_direct(shelf: dict, lm: dict) -> Optional[dict]:
    above = (lm["price"] - shelf["high"]) / shelf["high"] * 100
    if above < A_MIN_BREAKOUT_PCT:      return None
    if lm["rvol"] < A_MIN_RVOL:         return None
    if lm["volume"] < A_MIN_VOL_USDT:   return None
    if lm["pct_change"] < A_MIN_PCT_CHANGE: return None

    return {
        "above_pct": above,
        "rvol": lm["rvol"],
        "vol_vs_prev": lm["vol_vs_prev"],
        "volume": lm["volume"],
        "pct_change": lm["pct_change"],
        "price": lm["price"],
        "entry": lm["price"],
    }


def find_sweep(shelf: dict, closed: list[dict]) -> tuple[Optional[dict], int]:
    if len(closed) < B_SWEEP_LOOKBACK:
        return None, 0

    recent = closed[-B_SWEEP_LOOKBACK:]
    base_low = shelf["low"]
    base_avg = shelf["avg_vol"]

    for i in range(len(recent) - 1, -1, -1):
        c = recent[i]
        age = len(recent) - i
        if age > B_SWEEP_MAX_AGE:
            break

        # прокол ниже базы
        if c["low"] >= base_low * (1 - B_SWEEP_MIN_BREAK_PCT / 100):
            continue
        # закрылись обратно внутрь
        if B_SWEEP_MUST_CLOSE_IN and c["close"] < base_low:
            continue
        # объём сухой
        if c["volume"] > base_avg * B_SWEEP_MAX_VOL_RATIO:
            continue

        return c, age

    return None, 0


def check_sweep_expansion(
    shelf: dict,
    lm: dict,
    sweep: dict,
    sweep_age: int,
) -> Optional[dict]:
    if sweep["volume"] <= 0:
        return None

    exp_vs_sweep = lm["volume"] / sweep["volume"]
    if exp_vs_sweep < B_EXP_MIN_VS_SWEEP:
        return None

    above = (lm["price"] - shelf["high"]) / shelf["high"] * 100
    if above < -A_MIN_BREAKOUT_PCT:
        return None

    payload = {
        "above_pct": above,
        "rvol": lm["rvol"],
        "vol_vs_prev": lm["vol_vs_prev"],
        "volume": lm["volume"],
        "pct_change": lm["pct_change"],
        "price": lm["price"],
        "entry": lm["price"],
        "sweep_age": sweep_age,
        "sweep_vol_ratio": sweep["volume"] / shelf["avg_vol"],
        "exp_vs_sweep": exp_vs_sweep,
    }

    # EARLY
    if (B_EARLY_ENABLED
        and lm["vol_vs_prev"] >= B_EARLY_MIN_VS_PREV
        and lm["pct_change"] >= A_MIN_PCT_CHANGE):
        return {**payload, "level": "EARLY"}

    # CONFIRMED
    if (lm["vol_vs_prev"] >= B_EXP_MIN_VS_PREV
        and lm["rvol"] >= B_EXP_MIN_VS_AVG):
        return {**payload, "level": "CONFIRMED"}

    return None

# ═══════════════════════════════════════════════════════════════
#  TELEGRAM
# ═══════════════════════════════════════════════════════════════

async def tg(text: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID or not SESSION:
        return False
    try:
        async with SESSION.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            ok = r.status == 200
            if not ok:
                STATS["tg_fails"] += 1
            return ok
    except Exception:
        STATS["tg_fails"] += 1
        return False


def h_shelf(s: dict) -> str:
    return (
        f"📦 Полка: <code>{s['low']:.8g} — {s['high']:.8g}</code>\n"
        f"📐 Ширина: <b>{s['width_pct']:.2f}%</b> | "
        f"Свечей: <b>{s['candles']}×{BAR}</b>\n"
        f"📊 Avg vol: <b>${s['avg_vol']:,.0f}</b> | "
        f"Stab: <b>{s['vol_stability']:.2f}x</b>"
    )


async def tg_direct(base, shelf, sig, info):
    msg = (
        f"🚀 <b>ПРОБОЙ ПОЛКИ (direct)</b>\n\n"
        f"📌 <b>{escape(base)}USDT</b>\n"
        f"{h_shelf(shelf)}\n\n"
        f"💵 Цена: <code>{sig['price']:.8g}</code>\n"
        f"📈 Выше верха: <b>+{sig['above_pct']:.2f}%</b>\n"
        f"⚡ Vol vs prev: <b>{sig['vol_vs_prev']:.1f}x</b>\n"
        f"📊 RVOL: <b>{sig['rvol']:.2f}x</b>\n"
        f"💰 Залив: <b>${sig['volume']:,.0f}</b>\n"
        f"🌐 24h: <b>{info['change24']:+.1f}%</b> | "
        f"Vol: <b>${info['vol24']:,.0f}</b>"
    )
    if await tg(msg):
        STATS["sig_direct"] += 1


async def tg_sweep(base, shelf, sig, info):
    badge = "⚡ РАННИЙ" if sig["level"] == "EARLY" else "🎯 ПОДТВЕРЖДЁН"
    msg = (
        f"<b>SWEEP → ВЗРЫВ ОБЪЁМА</b> [{badge}]\n\n"
        f"📌 <b>{escape(base)}USDT</b>\n"
        f"{h_shelf(shelf)}\n\n"
        f"🧊 Sweep ({sig['sweep_age']} свечей назад):\n"
        f"   сухой объём <b>{sig['sweep_vol_ratio']*100:.1f}%</b> от полки\n\n"
        f"💥 <b>Взрыв:</b>\n"
        f"   vs prev: <b>{sig['vol_vs_prev']:.1f}x</b>\n"
        f"   vs avg: <b>{sig['rvol']:.2f}x</b>\n"
        f"   vs sweep: <b>{sig['exp_vs_sweep']:.1f}x</b>\n\n"
        f"💵 Цена: <code>{sig['price']:.8g}</code>\n"
        f"📈 Импульс: <b>{sig['above_pct']:+.2f}%</b>\n"
        f"💰 Залив: <b>${sig['volume']:,.0f}</b>"
    )
    if await tg(msg):
        if sig["level"] == "EARLY":
            STATS["sig_sweep_early"] += 1
        else:
            STATS["sig_sweep_confirmed"] += 1


async def tg_continuation(base, trk, lm):
    p = (lm["price"] - trk["entry"]) / trk["entry"] * 100
    msg = (
        f"✅ <b>CONTINUATION</b>\n\n"
        f"📌 <b>{escape(base)}USDT</b>\n"
        f"💰 Entry: <code>{trk['entry']:.8g}</code>\n"
        f"💵 Сейчас: <code>{lm['price']:.8g}</code>\n"
        f"📈 Профит: <b>+{p:.2f}%</b>\n"
        f"📊 RVOL: <b>{lm['rvol']:.2f}x</b>"
    )
    if await tg(msg):
        STATS["trk_cont"] += 1


async def tg_fakeout(base, trk, price):
    msg = (
        f"❌ <b>FAKEOUT — вернулась в полку</b>\n\n"
        f"📌 <b>{escape(base)}USDT</b>\n"
        f"💰 Entry: <code>{trk['entry']:.8g}</code>\n"
        f"💵 Сейчас: <code>{price:.8g}</code>\n"
        f"📉 Полка: <code>{trk['sh_low']:.8g} — {trk['sh_high']:.8g}</code>"
    )
    if await tg(msg):
        STATS["trk_fakeout"] += 1


async def tg_climax(base, trk, lm):
    p = (lm["price"] - trk["entry"]) / trk["entry"] * 100
    msg = (
        f"🥊 <b>CLIMAX — фиксация</b>\n\n"
        f"📌 <b>{escape(base)}USDT</b>\n"
        f"💰 Entry: <code>{trk['entry']:.8g}</code>\n"
        f"💵 Сейчас: <code>{lm['price']:.8g}</code>\n"
        f"📈 Профит: <b>+{p:.2f}%</b>\n"
        f"📊 RVOL: <b>{lm['rvol']:.2f}x</b>"
    )
    if await tg(msg):
        STATS["trk_climax"] += 1

# ═══════════════════════════════════════════════════════════════
#  START TRACK
# ═══════════════════════════════════════════════════════════════

def start_track(base: str, shelf: dict, entry: float, kind: str):
    TRACKS[base] = {
        "entry": entry,
        "kind": kind,
        "ts": time.time(),
        "sh_high": shelf["high"],
        "sh_low": shelf["low"],
        "max_price": entry,
        "flags": set(),
    }
    log.info("📍 TRACK %s entry=%.8g kind=%s", base, entry, kind)

# ═══════════════════════════════════════════════════════════════
#  PROCESS LIVE (по каждой полке + треку)
# ═══════════════════════════════════════════════════════════════

async def process_shelf(base: str):
    shelf = SHELVES.get(base)
    if not shelf or base in TRACKS:
        return
    info = UNIVERSE.get(base)
    if not info:
        return

    now = time.time()
    if now - LAST_SIGNAL.get(base, 0) < SIGNAL_COOLDOWN:
        return

    buf = list(CANDLES.get(base, []))
    if len(buf) < SH_MIN_CANDLES + 3:
        return

    # закрытые = все кроме последней
    closed = buf[:-1]
    lm = live_metrics(buf)

    # Ветка A
    if lm:
        sig = check_direct(shelf, lm)
        if sig:
            shelf["stage"] = "SIGNALED"
            LAST_SIGNAL[base] = now
            await tg_direct(base, shelf, sig, info)
            start_track(base, shelf, sig["entry"], "direct")
            return

    # Ветка B
    if not shelf.get("sweep"):
        sweep, age = find_sweep(shelf, closed)
        if sweep:
            shelf["sweep"] = sweep
            shelf["sweep_age"] = age
            shelf["sweep_ts"] = now
            log.info(
                "🧊 SWEEP %s | vol=$%.0f (%.1f%% of shelf)",
                base, sweep["volume"],
                sweep["volume"] / shelf["avg_vol"] * 100,
            )

    if shelf.get("sweep") and lm:
        age = int((now - shelf["sweep_ts"]) / BAR_SECONDS) + 1
        if age > B_SWEEP_MAX_AGE:
            shelf.pop("sweep", None)
            shelf.pop("sweep_ts", None)
            return

        sig = check_sweep_expansion(shelf, lm, shelf["sweep"], age)
        if sig:
            shelf["stage"] = "SIGNALED"
            LAST_SIGNAL[base] = now
            await tg_sweep(base, shelf, sig, info)
            start_track(base, shelf, sig["entry"], "sweep")
            return


async def process_track(base: str):
    trk = TRACKS.get(base)
    if not trk:
        return

    now = time.time()
    if now - trk["ts"] > TRK_TIMEOUT_SEC:
        TRACKS.pop(base, None)
        STATS["trk_timeout"] += 1
        return

    buf = list(CANDLES.get(base, []))
    if len(buf) < 3:
        return

    live = buf[-1]
    if live["open"] <= 0:
        return

    price = live["close"]
    trk["max_price"] = max(trk["max_price"], price)
    change = (price - trk["entry"]) / trk["entry"] * 100

    # Инвалидация
    if change <= TRK_INVALIDATE_PCT:
        TRACKS.pop(base, None)
        STATS["trk_inval"] += 1
        log.info("🛑 INVALIDATE %s (%.2f%%)", base, change)
        return

    # Fakeout
    back_inside = (trk["sh_high"] - price) / trk["sh_high"] * 100
    if ("fakeout" not in trk["flags"]
        and price < trk["sh_high"]
        and back_inside > TRK_FAKEOUT_PCT):
        await tg_fakeout(base, trk, price)
        trk["flags"].add("fakeout")
        TRACKS.pop(base, None)
        return

    lm = live_metrics(buf)
    if not lm:
        return

    # Continuation
    if ("cont" not in trk["flags"]
        and price > trk["sh_high"] * (1 + TRK_HOLD_TOL_PCT / 100)
        and change >= TRK_PROFIT_ALERT):
        await tg_continuation(base, trk, lm)
        trk["flags"].add("cont")

    # Climax
    if "climax" not in trk["flags"]:
        rng = live["high"] - live["low"]
        if rng > 0 and lm["rvol"] >= CLX_MIN_RVOL:
            upper_wick = live["high"] - max(live["open"], live["close"])
            wick = upper_wick / rng
            peak = (live["high"] - trk["entry"]) / trk["entry"] * 100
            if wick >= CLX_MIN_WICK and peak >= CLX_MIN_PROFIT:
                await tg_climax(base, trk, lm)
                trk["flags"].add("climax")
                TRACKS.pop(base, None)

# ═══════════════════════════════════════════════════════════════
#  SCAN LOOPS
# ═══════════════════════════════════════════════════════════════

async def refresh_shelves():
    """Обновляем полки раз в N циклов — тяжелее, делаем редко."""
    if not UNIVERSE:
        return

    found = {}
    for base in list(UNIVERSE.keys()):
        buf = list(CANDLES.get(base, []))
        if len(buf) < SH_MIN_CANDLES + 2:
            continue
        closed = buf[:-1]
        shelf = detect_shelf(closed)
        if not shelf:
            continue

        old = SHELVES.get(base)
        if old:
            shelf["created_ts"] = old.get("created_ts", time.time())
            shelf["stage"] = old.get("stage", "SHELF")
            shelf["sweep"] = old.get("sweep")
            shelf["sweep_age"] = old.get("sweep_age")
            shelf["sweep_ts"] = old.get("sweep_ts")
        else:
            shelf["created_ts"] = time.time()
            shelf["stage"] = "SHELF"

        shelf["base"] = base
        found[base] = shelf

    # удаляем устаревшие
    now = time.time()
    for base, sh in list(found.items()):
        if now - sh["created_ts"] > SH_MAX_AGE_SEC and base not in TRACKS:
            del found[base]

    SHELVES.clear()
    SHELVES.update(found)


async def scan_cycle():
    if not UNIVERSE:
        return
    STATS["scans"] += 1

    # 1) Обновить полки
    await refresh_shelves()

    # 2) Прогнать live-проверку по полкам и трекам
    bases = set(SHELVES.keys()) | set(TRACKS.keys())
    if not bases:
        return

    sem = asyncio.Semaphore(8)

    async def worker(b):
        async with sem:
            try:
                await process_shelf(b)
                await process_track(b)
            except Exception:
                STATS["errors"] += 1

    await asyncio.gather(*[worker(b) for b in bases])


async def scan_loop():
    while True:
        try:
            await scan_cycle()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            STATS["errors"] += 1
            log.exception("scan_cycle: %s", e)
        await asyncio.sleep(SCAN_INTERVAL_SEC)


async def universe_loop():
    # первичная загрузка
    await refresh_universe()
    while True:
        try:
            await asyncio.sleep(UNIVERSE_REFRESH)
            await refresh_universe()
            sync_ws_subscriptions()
        except asyncio.CancelledError:
            raise
        except Exception:
            STATS["errors"] += 1
            await asyncio.sleep(15)

# ═══════════════════════════════════════════════════════════════
#  WEB
# ═══════════════════════════════════════════════════════════════

async def index(_req):
    uptime = (time.time() - START_TS) / 3600
    text = (
        f"PUMP-SCREENER v1.0\n"
        f"uptime: {uptime:.2f}h\n"
        f"universe: {len(UNIVERSE)}\n"
        f"shelves: {len(SHELVES)}\n"
        f"tracks: {len(TRACKS)}\n"
        f"ws_subs: {len(WS_SUBSCRIBED)}\n"
        f"candles: {STATS['candles_closed']}\n"
        f"signals: direct={STATS['sig_direct']} "
        f"sweep_e={STATS['sig_sweep_early']} "
        f"sweep_c={STATS['sig_sweep_confirmed']}\n"
        f"track: cont={STATS['trk_cont']} "
        f"fakeout={STATS['trk_fakeout']} "
        f"climax={STATS['trk_climax']}\n"
        f"http_429: {STATS['http_429']} | "
        f"ws_reconn: {STATS['ws_reconnects']} | "
        f"errors: {STATS['errors']}"
    )
    return web.Response(text=text, content_type="text/plain")


async def health(_req):
    return web.json_response({
        "ok": True,
        "universe": len(UNIVERSE),
        "shelves": len(SHELVES),
        "tracks": len(TRACKS),
        "ws_subs": len(WS_SUBSCRIBED),
        "stats": dict(STATS),
        "uptime": time.time() - START_TS,
    })

# ═══════════════════════════════════════════════════════════════
#  STARTUP / SHUTDOWN
# ═══════════════════════════════════════════════════════════════

async def on_start(app: web.Application):
    global SESSION, HTTP_SEM, WS_TASK

    SESSION = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
    )
    HTTP_SEM = asyncio.Semaphore(15)

    if BOT_TOKEN:
        try:
            async with SESSION.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook",
                params={"drop_pending_updates": "false"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                log.info("deleteWebhook: HTTP %s", r.status)
        except Exception as e:
            log.warning("deleteWebhook: %s", e)

    await load_instruments()

    WS_TASK = asyncio.create_task(websocket_loop())
    app["ws_task"] = WS_TASK

    app["uni_task"] = asyncio.create_task(universe_loop())
    app["scan_task"] = asyncio.create_task(scan_loop())

    await tg(
        "🚀 <b>PUMP-SCREENER v1.0 STARTED</b>\n"
        "WS-first · 5m candles\n"
        f"• Universe cap: {MAX_UNIVERSE}\n"
        f"• Poll: REST tickers / {UNIVERSE_REFRESH}s · WS candles realtime\n"
        f"• Scan: {SCAN_INTERVAL_SEC}s"
    )


async def on_stop(app: web.Application):
    for key in ("ws_task", "uni_task", "scan_task"):
        t = app.get(key)
        if t:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
    if SESSION and not SESSION.closed:
        await SESSION.close()


def main():
    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/health", health)
    app.on_startup.append(on_start)
    app.on_cleanup.append(on_stop)
    web.run_app(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
