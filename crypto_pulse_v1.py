import os
import time
import asyncio
import logging
from collections import deque

import aiohttp
from flask import Flask
import threading


# ============================================================
#                    CONFIGURATION
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")
PORT = int(os.environ.get("PORT", 10000))


# ============================================================
#              ОСНОВНЫЕ НАСТРОЙКИ CRYPTO_PULSE
# ============================================================

WINDOW_MINUTES = 60
PRICE_CHANGE_PCT = 4.0
PRICE_CHECK_INTERVAL = 30
MIN_24H_VOLUME_USDT = 800_000
ALERT_COOLDOWN_SECONDS = 1800


# ============================================================
#                 RVOL И ЗАЩИТНЫЕ ФИЛЬТРЫ
# ============================================================

RVOL_FILTER_ENABLED = True

MIN_HOURLY_RVOL = 0.9
MIN_SHORT_RVOL = 0.74

# МИНИМАЛЬНЫЙ ОТКРЫТЫЙ ИНТЕРЕС В USDT ($10,000,000)
MIN_OPEN_INTEREST_USDT = 10_000_000

MIN_CANDLES_REQUIRED = 12


# ============================================================
#                 КОРОТКИЙ RVOL
# ============================================================

SHORT_RVOL_INTERVAL = "5m"
SHORT_RVOL_LOOKBACK = 12
SHORT_RVOL_RECENT_COUNT = 3

LOOKBACK_SECONDS = WINDOW_MINUTES * 60

MAX_HISTORY_POINTS = int(
    (LOOKBACK_SECONDS * 2) / PRICE_CHECK_INTERVAL
)


# ============================================================
#          ФИЛЬТР НЕ-КРИПТОВЫХ ИНСТРУМЕНТОВ
# ============================================================

NON_CRYPTO_PREFIXES = (
    "NCS", "NCCO", "SP500", "US30", "US100", "NAS100",
    "GER40", "UK100", "JPN225", "GOLD", "SILVER",
    "BRENT", "WTI", "OIL", "XAU", "XAG", "XTI",
    "XBR", "EUR", "GBP", "JPY", "AUD", "CAD",
    "CHF", "HK", "DXY"
)

EXCLUDED_SYMBOLS = {
    "USDC-USDT",
    "FDUSD-USDT",
    "USD1-USDT"
}


# ============================================================
#                      MEMORY
# ============================================================

PRICE_HISTORY = {}
ALERT_STATE = {}
ACTIVE_SYMBOLS = set()
VALID_FUTURES_SYMBOLS = set()


# ============================================================
#                     LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# ============================================================
#                     FLASK
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():
    return (
        f"CryptoPulse | "
        f"Lookback={WINDOW_MINUTES}m | "
        f"Threshold={PRICE_CHANGE_PCT:.2f}% | "
        f"Symbols={len(ACTIVE_SYMBOLS)} | "
        f"Min_OI=${MIN_OPEN_INTEREST_USDT:,.0f}"
    ), 200


def run_flask():
    app.run(
        host="0.0.0.0",
        port=PORT
    )


# ============================================================
#                 HELPERS
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
    if not symbol:
        return False

    if not symbol.endswith("-USDT"):
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
#             UNIVERSAL KLINE PARSER
# ============================================================

def parse_kline(k):
    try:
        if isinstance(k, dict):
            timestamp = (
                k.get("time")
                or k.get("timestamp")
                or k.get("openTime")
                or 0
            )
            o = float(k.get("open", 0))
            h = float(k.get("high", 0))
            l = float(k.get("low", 0))
            c = float(k.get("close", 0))
            v = float(k.get("volume", 0))

            return int(timestamp), o, h, l, c, v

        if isinstance(k, (list, tuple)):
            timestamp = int(k[0])
            o = float(k[1])
            h = float(k[2])
            l = float(k[3])
            c = float(k[4])
            v = float(k[5])

            return timestamp, o, h, l, c, v

    except (IndexError, KeyError, TypeError, ValueError):
        pass

    return 0, 0.0, 0.0, 0.0, 0.0, 0.0


# ============================================================
#                 TELEGRAM
# ============================================================

async def send_telegram_alert(session, text):
    if BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN" or CHAT_ID == "YOUR_TELEGRAM_CHAT_ID":
        logging.info("[TG MOCK ALERT]\n%s", text)
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }

    try:
        async with session.post(url, json=payload, timeout=10) as resp:
            if resp.status != 200:
                logging.error("Telegram HTTP %s: %s", resp.status, await resp.text())
                return False
            return True
    except Exception as e:
        logging.error("Telegram error: %s", e)
        return False


# ============================================================
#                 STARTUP MESSAGE
# ============================================================

async def send_startup_message(session):
    message = (
        "🟢 <b>Crypto_pulse запущен (Режим: OI ≥ $10M, Без Истории)</b>\n\n"
        f"📈 Поиск импульса: <b>±{PRICE_CHANGE_PCT:.1f}%</b>\n"
        f"⏱ Окно поиска: <b>{WINDOW_MINUTES} минут</b>\n"
        f"🔄 Проверка рынка: <b>{PRICE_CHECK_INTERVAL} сек</b>\n"
        f"💵 Мин. объем 24ч: <b>${MIN_24H_VOLUME_USDT:,.0f}</b>\n"
        f"📊 RVOL 1H ≥ <b>{MIN_HOURLY_RVOL}</b> | Short RVOL 5m ≥ <b>{MIN_SHORT_RVOL}</b>\n"
        f"🎯 Фильтр ОИ: <b>≥ ${MIN_OPEN_INTEREST_USDT:,.0f} USDT</b> (Мгновенный запрос)\n\n"
        "🚀 <i>Мониторинг рынка начат. Сигналы отправляются без ожидания накопления ОИ!</i>"
    )
    await send_telegram_alert(session, message)


# ============================================================
#            BINGX FUTURES CONTRACTS
# ============================================================

async def update_futures_symbols(session):
    global VALID_FUTURES_SYMBOLS
    url = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        async with session.get(url, headers=headers, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("code") == 0:
                    symbols = set()
                    for item in data.get("data", []):
                        sym = normalize_symbol(item.get("symbol"))
                        if sym and is_crypto_usdt_symbol(sym):
                            symbols.add(sym)

                    if symbols:
                        VALID_FUTURES_SYMBOLS = symbols
                        logging.info("✅ Загружено активных ФЬЮЧЕРСОВ BingX: %d", len(VALID_FUTURES_SYMBOLS))
    except Exception as e:
        logging.warning("⚠️ Ошибка обновления списка фьючерсов: %s", e)


# ============================================================
#                 BINGX TICKERS
# ============================================================

async def get_market_tickers(session):
    url = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        async with session.get(url, headers=headers, timeout=10) as resp:
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

                if VALID_FUTURES_SYMBOLS and symbol not in VALID_FUTURES_SYMBOLS:
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
#                 BINGX KLINES
# ============================================================

async def get_klines(session, symbol, interval, limit=21):
    url = "https://open-api.bingx.com/openApi/swap/v3/quote/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        async with session.get(url, params=params, headers=headers, timeout=8) as resp:
            if resp.status != 200:
                return []

            data = await resp.json()
            if data.get("code") != 0:
                return []

            candles = data.get("data", [])
            if not isinstance(candles, list) or len(candles) < 3:
                return []

            candles = sorted(candles, key=lambda x: parse_kline(x)[0])
            return candles
    except Exception:
        return []


# ============================================================
#          НАДЕЖНЫЙ МЕТОД ПОЛУЧЕНИЯ ТЕКУЩЕГО OI В USDT
# ============================================================

async def fetch_current_open_interest_usdt(session, symbol, current_price):
    """
    Запрашивается ТОЛЬКО по монетам, пробившим ±4% и прошедшим RVOL.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"
    }

    symbol_variants = [symbol, symbol.replace("-", "")]

    for sym_param in symbol_variants:
        url = "https://open-api.bingx.com/openApi/swap/v2/quote/openInterest"
        params = {"symbol": sym_param}

        try:
            async with session.get(url, params=params, headers=headers, timeout=5) as resp:
                if resp.status != 200:
                    continue

                data = await resp.json()
                if data.get("code") != 0:
                    continue

                oi_data = data.get("data")
                if isinstance(oi_data, list) and len(oi_data) > 0:
                    oi_data = oi_data[0]

                if isinstance(oi_data, dict):
                    oi_val = (
                        oi_data.get("openInterestValue") 
                        or oi_data.get("openInterest") 
                        or oi_data.get("value")
                    )

                    if oi_val is not None:
                        oi_float = float(oi_val)
                        if oi_float > 0:
                            # Если отдан объем в монетах — переводим в USDT
                            if oi_float < 500_000 and current_price > 0:
                                return oi_float * current_price
                            return oi_float
        except Exception:
            pass

    return None


# ============================================================
#                  HOURLY RVOL
# ============================================================

async def fetch_hourly_rvol(session, symbol):
    klines = await get_klines(session, symbol, "1h", 21)
    if len(klines) < MIN_CANDLES_REQUIRED:
        return 0.0

    volumes = []
    for k in klines:
        _, _, _, _, close, volume = parse_kline(k)
        if close <= 0 or volume <= 0:
            continue
        volumes.append(volume * close)

    if len(volumes) < 5:
        return 1.0

    current_volume = volumes[-1]
    previous = volumes[:-1]
    avg_volume = sum(previous) / len(previous)

    if avg_volume <= 0:
        return 1.0

    now = time.time()
    candle_start = (int(now) // 3600) * 3600
    elapsed = max(1, now - candle_start)
    fraction = max(0.1, min(elapsed / 3600.0, 1.0))

    projected = current_volume / fraction
    return projected / avg_volume


# ============================================================
#                 SHORT RVOL 5M
# ============================================================

async def fetch_short_rvol(session, symbol):
    klines = await get_klines(session, symbol, SHORT_RVOL_INTERVAL, SHORT_RVOL_LOOKBACK + 6)
    if len(klines) < 5:
        return 1.0

    volumes = []
    for k in klines:
        _, _, _, _, close, volume = parse_kline(k)
        if close <= 0 or volume <= 0:
            continue
        volumes.append(volume * close)

    if len(volumes) < 5:
        return 1.0

    recent_count = min(SHORT_RVOL_RECENT_COUNT, len(volumes))
    recent = volumes[-recent_count:]

    historical_end = len(volumes) - recent_count
    historical_start = max(0, historical_end - SHORT_RVOL_LOOKBACK)
    historical = volumes[historical_start:historical_end]

    if not historical:
        return 1.0

    avg_volume = sum(historical) / len(historical)
    if avg_volume <= 0:
        return 1.0

    recent_average = sum(recent) / len(recent)
    return recent_average / avg_volume


# ============================================================
#                 PRICE HISTORY
# ============================================================

def add_price_sample(symbol, timestamp, price):
    history = PRICE_HISTORY.get(symbol)
    if history is None:
        history = deque(maxlen=MAX_HISTORY_POINTS)
        PRICE_HISTORY[symbol] = history

    history.append((timestamp, price))


def get_oldest_price_in_window(history, target_timestamp):
    if not history:
        return None, None

    oldest_t = history[0][0]
    oldest_p = history[0][1]

    for t, p in history:
        if t <= target_timestamp:
            oldest_t = t
            oldest_p = p
        else:
            break

    return oldest_t, oldest_p


# ============================================================
#                 ALERT LOGIC
# ============================================================

def check_alert_conditions(symbol, change_pct, now_timestamp):
    state = ALERT_STATE.get(symbol)
    if not state:
        return True, 1

    prev_change = state["change_pct"]
    prev_time = state["time"]
    last_step = state.get("step", 1)

    if (prev_change > 0 and change_pct < 0) or (prev_change < 0 and change_pct > 0):
        return True, 1

    current_step = int(abs(change_pct) // PRICE_CHANGE_PCT)
    if current_step > last_step:
        return True, current_step

    if (now_timestamp - prev_time) >= ALERT_COOLDOWN_SECONDS:
        return True, current_step

    return False, last_step


# ============================================================
#                 PROCESS TICKERS
# ============================================================

async def process_tickers(session, tickers):
    now_timestamp = time.time()
    target_timestamp = now_timestamp - LOOKBACK_SECONDS

    alerts = 0
    ready = 0
    filtered_oi = 0

    for (symbol, current_price, quote_volume) in tickers:
        add_price_sample(symbol, now_timestamp, current_price)
        history = PRICE_HISTORY[symbol]

        old_t, old_price = get_oldest_price_in_window(history, target_timestamp)

        if old_price is None or old_price <= 0:
            continue

        if (now_timestamp - old_t) < LOOKBACK_SECONDS * 0.8:
            continue

        actual_minutes = max(1, int((now_timestamp - old_t) / 60))
        ready += 1

        # 1. ПРОВЕРКА ИМПУЛЬСА ЦЕНЫ (±4%)
        change_pct = ((current_price - old_price) / old_price) * 100.0

        if abs(change_pct) < PRICE_CHANGE_PCT:
            continue

        should_alert, step_level = check_alert_conditions(symbol, change_pct, now_timestamp)
        if not should_alert:
            continue

        # 2. ПРОВЕРКА RVOL
        hourly_rvol = await fetch_hourly_rvol(session, symbol)
        short_rvol = await fetch_short_rvol(session, symbol)

        if RVOL_FILTER_ENABLED:
            if hourly_rvol < MIN_HOURLY_RVOL or short_rvol < MIN_SHORT_RVOL:
                continue

        # 3. ПОЛУЧЕНИЕ ТЕКУЩЕГО ОИ И ПРОВЕРКА ПОРОГА $10M
        oi_usdt = await fetch_current_open_interest_usdt(session, symbol, current_price)

        if oi_usdt is None or oi_usdt < MIN_OPEN_INTEREST_USDT:
            filtered_oi += 1
            logging.info(
                "🛡 Отфильтровано по OI | %s | OI: %s | Мин.требуется: $%s",
                symbol,
                f"${oi_usdt:,.0f}" if oi_usdt else "None",
                f"{MIN_OPEN_INTEREST_USDT:,.0f}"
            )
            continue

        # ФОРМИРОВАНИЕ И ОТПРАВКА СИГНАЛА
        if change_pct > 0:
            direction = "UP"
            emoji = "🚀"
            sign = "+"
        else:
            direction = "DOWN"
            emoji = "🔻"
            sign = ""

        clean_coin = symbol.split("-")[0]

        if short_rvol >= 5:
            rvol_comment = "🔥 ОЧЕНЬ сильный краткосрочный объём"
        elif short_rvol >= 3:
            rvol_comment = "⚡ Сильный краткосрочный объём"
        elif short_rvol >= 2:
            rvol_comment = "📈 Повышенный краткосрочный объём"
        elif short_rvol >= 1:
            rvol_comment = "📊 Объём выше/около нормы"
        else:
            rvol_comment = "💤 Краткосрочный объём слабый"

        message = (
            f"<code>{clean_coin}</code>\n\n"
            f"{emoji} <b>{clean_coin}USDT</b>\n\n"
            f"📈 <b>Изменение за ~{actual_minutes}м:</b> {sign}{change_pct:.2f}%\n"
            f"📊 <b>RVOL 1H:</b> {hourly_rvol:.2f}x\n"
            f"🔥 <b>Short RVOL 5m:</b> {short_rvol:.2f}x\n"
            f"{rvol_comment}\n\n"
            f"👁 <b>Открытый интерес:</b> ${oi_usdt:,.0f}\n"
            f"💰 <b>Текущая цена:</b> {format_price(current_price)}\n"
            f"⏪ <b>Старая цена:</b> {format_price(old_price)}\n"
            f"💵 <b>Объём 24ч:</b> ${quote_volume:,.0f}\n\n"
            f"⚡ <i>Импульс достиг {PRICE_CHANGE_PCT:.2f}% (Шаг {step_level})</i>"
        )

        sent = await send_telegram_alert(session, message)

        ALERT_STATE[symbol] = {
            "time": now_timestamp,
            "change_pct": change_pct,
            "direction": direction,
            "step": step_level
        }

        alerts += 1
        logging.info(
            "🚀 СИГНАЛ | %s | %s%.2f%% за %dm | RVOL 1H %.2fx | Short RVOL %.2fx | OI $%.0f",
            clean_coin, sign, change_pct, actual_minutes, hourly_rvol, short_rvol, oi_usdt
        )

    return ready, alerts, filtered_oi


# ============================================================
#                     MAIN LOOP
# ============================================================

async def main_loop():
    global ACTIVE_SYMBOLS

    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        logging.info("🚀 CRYPTO_PULSE ЗАПУЩЕН (Прямой OI >= $10M)")

        await update_futures_symbols(session)
        await send_startup_message(session)

        loop_count = 0

        while True:
            started = time.time()
            loop_count += 1

            if loop_count % 120 == 0:
                await update_futures_symbols(session)

            tickers = await get_market_tickers(session)

            if tickers:
                ACTIVE_SYMBOLS = {item[0] for item in tickers}

                ready, alerts, filtered_oi = await process_tickers(session, tickers)

                logging.info(
                    "📊 СКАН | пар=%d | готово=%d | отфильтровано OI=%d | сигналов=%d | история цены=%d",
                    len(tickers), ready, filtered_oi, alerts, len(PRICE_HISTORY)
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
