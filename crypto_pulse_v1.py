import os
import time
import asyncio
import logging
from datetime import datetime, timezone
from collections import deque

import aiohttp
from flask import Flask
import threading


# ============================================================
#                    CONFIGURATION
# ============================================================

BOT_TOKEN = os.environ.get(
    "BOT_TOKEN",
    "YOUR_TELEGRAM_BOT_TOKEN"
)

CHAT_ID = os.environ.get(
    "CHAT_ID",
    "YOUR_TELEGRAM_CHAT_ID"
)

PORT = int(os.environ.get("PORT", 10000))


# ============================================================
#                 ОСНОВНЫЕ НАСТРОЙКИ ИМПУЛЬСА
# ============================================================

PRICE_CHANGE_TIMEFRAME = "1h"
PRICE_CHANGE_PCT = 4.0
PRICE_CHECK_INTERVAL = 30
MIN_24H_VOLUME_USDT = 2_000_000
ALERT_COOLDOWN_SECONDS = 1800  # 30 минут
REARM_PCT = PRICE_CHANGE_PCT * 0.50
LOG_EVERY_SCAN = False


# ============================================================
#                 ДОПУСТИМЫЕ ПЕРИОДЫ
# ============================================================

SUPPORTED_TIMEFRAMES = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
}

if PRICE_CHANGE_TIMEFRAME not in SUPPORTED_TIMEFRAMES:
    raise ValueError(
        "PRICE_CHANGE_TIMEFRAME должен быть одним из: "
        + ", ".join(SUPPORTED_TIMEFRAMES.keys())
    )

if PRICE_CHANGE_PCT <= 0:
    raise ValueError("PRICE_CHANGE_PCT должен быть больше 0")

PERIOD_SECONDS = SUPPORTED_TIMEFRAMES[PRICE_CHANGE_TIMEFRAME]

# Запас истории увеличен до 4 часов (480 точек при 30 сек)
MAX_HISTORY_POINTS = int((PERIOD_SECONDS * 4) / PRICE_CHECK_INTERVAL)


# ============================================================
#          ФИЛЬТР НЕ-КРИПТОВЫХ ИНСТРУМЕНТОВ
# ============================================================

NON_CRYPTO_PREFIXES = (
    "NCS", "NCCO", "SP500", "US30", "US100", "NAS100", "GER40",
    "UK100", "JPN225", "GOLD", "SILVER", "BRENT", "WTI", "OIL",
    "XAU", "XAG", "XTI", "XBR", "EUR", "GBP", "JPY", "AUD", "CAD",
    "CHF", "HK", "DXY"
)

EXCLUDED_SYMBOLS = {
    "USDC-USDT", "FDUSD-USDT", "USD1-USDT"
}


# ============================================================
#                      MEMORY
# ============================================================

PRICE_HISTORY = {}
ALERT_STATE = {}
ACTIVE_SYMBOLS = set()


# ============================================================
#                     LOGGING & FLASK
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

app = Flask(__name__)

@app.route("/")
def home():
    return (
        f"ConsolidationHunter | "
        f"TF={PRICE_CHANGE_TIMEFRAME} | "
        f"Threshold={PRICE_CHANGE_PCT:.2f}% | "
        f"Symbols={len(ACTIVE_SYMBOLS)} | "
        f"History={len(PRICE_HISTORY)}"
    ), 200

def run_flask():
    app.run(host="0.0.0.0", port=PORT)


# ============================================================
#                 SYMBOL NORMALIZATION & HELPERS
# ============================================================

def normalize_symbol(raw):
    symbol = str(raw or "").strip().upper()
    if not symbol:
        return None
    if symbol.endswith("-USDT"):
        return symbol
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}-USDT"
    return None

def is_crypto_usdt_symbol(symbol):
    if not symbol or not symbol.endswith("-USDT"):
        return False
    if symbol in EXCLUDED_SYMBOLS:
        return False
    base = symbol[:-5]
    if any(base.startswith(prefix) for prefix in NON_CRYPTO_PREFIXES):
        return False
    if any(char in base for char in ("(", ")", "/")):
        return False
    return True

def format_price(price):
    if price >= 1000:
        return f"{price:.2f}"
    if price >= 1:
        return f"{price:.4f}"
    if price >= 0.01:
        return f"{price:.6f}"
    if price >= 0.0001:
        return f"{price:.8f}"
    return f"{price:.10f}"


# ============================================================
#                 TELEGRAM & BINGX API
# ============================================================

async def send_telegram_alert(session, text):
    if BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN" or CHAT_ID == "YOUR_TELEGRAM_CHAT_ID":
        logging.info("[TG MOCK ALERT]\n%s", text)
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}

    try:
        async with session.post(url, json=payload, timeout=10) as resp:
            if resp.status != 200:
                logging.error("Telegram HTTP %s: %s", resp.status, await resp.text())
    except Exception as e:
        logging.error("Telegram error: %s", e)


async def get_market_tickers(session):
    url = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            if data.get("code") != 0:
                return []

            result = []
            for item in data.get("data", []):
                symbol = normalize_symbol(item.get("symbol"))
                if not is_crypto_usdt_symbol(symbol):
                    continue
                try:
                    price = float(item.get("lastPrice", 0))
                    quote_volume = float(item.get("quoteVolume", 0))
                except (TypeError, ValueError):
                    continue

                if price <= 0 or quote_volume < MIN_24H_VOLUME_USDT:
                    continue

                result.append((symbol, price, quote_volume))
            return result
    except Exception as e:
        logging.warning("❌ BingX ticker error: %s", e)
        return []


# ============================================================
#       ГОРЯЧИЙ СТАРТ: ПРЕФИЛЛ ИСТОРИИ ИЗ СВЕЧЕЙ (KLINES)
# ============================================================

async def prefill_history_for_symbol(session, symbol, sem):
    """Подгружаем свечи для быстрой инициализации истории после деплоя."""
    async with sem:
        url = "https://open-api.bingx.com/openApi/swap/v3/quote/klines"
        params = {
            "symbol": symbol,
            "interval": "1h",
            "limit": 3
        }
        try:
            async with session.get(url, params=params, timeout=5) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
                if data.get("code") != 0 or not data.get("data"):
                    return

                # Перебираем полученные свечи от старых к новым
                klines = data.get("data", [])
                for k in reversed(klines):
                    # time может быть полем time или openTime
                    t_ms = float(k.get("time", k.get("openTime", 0)))
                    p_open = float(k.get("open", 0))
                    if t_ms > 0 and p_open > 0:
                        add_price_sample(symbol, t_ms / 1000.0, p_open)
        except Exception:
            pass


async def prefill_all_history(session, tickers):
    logging.info("🔥 Начинаем горячий запуск истории (запрос свечей)...")
    sem = asyncio.Semaphore(15)  # ограничение одновременных запросов
    tasks = [prefill_history_for_symbol(session, t[0], sem) for t in tickers]
    await asyncio.gather(*tasks)
    logging.info("✅ История успешно сформирована! Бот готов к выдаче сигналов.")


# ============================================================
#                 PRICE HISTORY & ALERTS
# ============================================================

def add_price_sample(symbol, timestamp, price):
    history = PRICE_HISTORY.get(symbol)
    if history is None:
        history = deque(maxlen=MAX_HISTORY_POINTS)
        PRICE_HISTORY[symbol] = history

    if history and timestamp <= history[-1][0]:
        return

    history.append((timestamp, price))


def price_at(history, target_timestamp):
    if not history or target_timestamp < history[0][0]:
        return None

    if target_timestamp >= history[-1][0]:
        return history[-1][1]

    points = list(history)
    prev_t, prev_p = points[0]

    for curr_t, curr_p in points[1:]:
        if curr_t >= target_timestamp:
            if curr_t == prev_t:
                return curr_p
            ratio = (target_timestamp - prev_t) / (curr_t - prev_t)
            return prev_p + (curr_p - prev_p) * ratio
        prev_t, prev_p = curr_t, curr_p

    return None


def can_alert(symbol, change_pct, now_timestamp):
    state = ALERT_STATE.get(symbol)
    if not state:
        return True

    previous_change = state["change_pct"]
    if (previous_change > 0 and change_pct < 0) or (previous_change < 0 and change_pct > 0):
        return True

    if abs(change_pct) <= REARM_PCT:
        return True

    if now_timestamp - state["time"] >= ALERT_COOLDOWN_SECONDS:
        return True

    return False


async def process_tickers(session, tickers):
    now_timestamp = time.time()
    target_timestamp = now_timestamp - PERIOD_SECONDS

    alerts = 0
    ready = 0
    strongest_move = None

    for symbol, current_price, quote_volume in tickers:
        add_price_sample(symbol, now_timestamp, current_price)
        history = PRICE_HISTORY[symbol]

        old_price = price_at(history, target_timestamp)
        if old_price is None or old_price <= 0:
            continue

        ready += 1
        change_pct = ((current_price - old_price) / old_price) * 100.0

        if strongest_move is None or abs(change_pct) > abs(strongest_move[1]):
            strongest_move = (symbol, change_pct)

        if abs(change_pct) < PRICE_CHANGE_PCT:
            continue

        direction = "UP" if change_pct > 0 else "DOWN"
        if not can_alert(symbol, change_pct, now_timestamp):
            continue

        emoji = "🚀" if change_pct > 0 else "🔻"
        sign = "+" if change_pct > 0 else ""
        clean_symbol = symbol.replace("-", "")

        message = (
            f"{emoji} <b>{clean_symbol}</b>\n\n"
            f"📈 <b>Изменение за {PRICE_CHANGE_TIMEFRAME}:</b> {sign}{change_pct:.2f}%\n"
            f"💰 <b>Текущая цена:</b> {format_price(current_price)}\n"
            f"⏪ <b>Цена {PRICE_CHANGE_TIMEFRAME} назад:</b> {format_price(old_price)}\n"
            f"💵 <b>Объём 24ч:</b> ${quote_volume:,.0f}\n\n"
            f"⚡ <i>Импульс достиг порога {PRICE_CHANGE_PCT:.2f}%</i>"
        )

        await send_telegram_alert(session, message)
        ALERT_STATE[symbol] = {
            "time": now_timestamp,
            "change_pct": change_pct,
            "direction": direction
        }
        alerts += 1

        logging.info("🚀 СИГНАЛ | %s | %s%.2f%% за %s", clean_symbol, sign, change_pct, PRICE_CHANGE_TIMEFRAME)

    return ready, alerts


# ============================================================
#                     MAIN LOOP
# ============================================================

async def main_loop():
    global ACTIVE_SYMBOLS
    timeout = aiohttp.ClientTimeout(total=15)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        logging.info("🚀 CONSOLIDATION HUNTER ЗАПУЩЕН")
        logging.info("⏱ Период: %s | Порог: %.2f%%", PRICE_CHANGE_TIMEFRAME, PRICE_CHANGE_PCT)

        # Первый запуск — собираем тикеры и подгружаем исторические свечи
        initial_tickers = await get_market_tickers(session)
        if initial_tickers:
            ACTIVE_SYMBOLS = {item[0] for item in initial_tickers}
            await prefill_all_history(session, initial_tickers)

        while True:
            started = time.time()
            tickers = await get_market_tickers(session)

            if tickers:
                ACTIVE_SYMBOLS = {item[0] for item in tickers}
                ready, alerts = await process_tickers(session, tickers)
                logging.info(
                    "📊 СКАН | пар=%d | готово=%d | сигналов=%d | история=%d",
                    len(tickers), ready, alerts, len(PRICE_HISTORY)
                )
            else:
                logging.warning("⚠️ Ticker BingX не получен.")

            elapsed = time.time() - started
            sleep_time = max(1, PRICE_CHECK_INTERVAL - elapsed)
            await asyncio.sleep(sleep_time)


# ============================================================
#                       START
# ============================================================

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    asyncio.run(main_loop())
