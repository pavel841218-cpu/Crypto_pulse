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

# За какой период ищем изменение цены
LOOKBACK_MINUTES = 60

# Основной импульсный порог
PRICE_CHANGE_PCT = 4.0

# Как часто сканируем рынок
PRICE_CHECK_INTERVAL = 30

# Минимальный 24h оборот
MIN_24H_VOLUME_USDT = 2_000_000

# Кулдаун повторного сигнала
ALERT_COOLDOWN_SECONDS = 1800


# ============================================================
#                 КОРОТКИЙ RVOL
# ============================================================

# Интервал короткого RVOL
SHORT_RVOL_INTERVAL = "5m"

# Сколько предыдущих 5m свечей использовать для среднего
SHORT_RVOL_LOOKBACK = 12

# Сколько последних 5m свечей дополнительно проверяем
SHORT_RVOL_RECENT_COUNT = 3


LOOKBACK_SECONDS = LOOKBACK_MINUTES * 60

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
        f"Lookback={LOOKBACK_MINUTES}m | "
        f"Threshold={PRICE_CHANGE_PCT:.2f}% | "
        f"Symbols={len(ACTIVE_SYMBOLS)}"
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

    if any(
        base.startswith(prefix)
        for prefix in NON_CRYPTO_PREFIXES
    ):
        return False

    if any(
        char in base
        for char in ("(", ")", "/")
    ):
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

        # BingX может вернуть dict
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

            return (
                int(timestamp),
                o,
                h,
                l,
                c,
                v
            )

        # Или list/tuple
        if isinstance(k, (list, tuple)):

            timestamp = int(k[0])

            o = float(k[1])
            h = float(k[2])
            l = float(k[3])
            c = float(k[4])
            v = float(k[5])

            return (
                timestamp,
                o,
                h,
                l,
                c,
                v
            )

    except (
        IndexError,
        KeyError,
        TypeError,
        ValueError
    ):
        pass

    return (
        0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0
    )


# ============================================================
#                 TELEGRAM
# ============================================================

async def send_telegram_alert(session, text):

    if (
        BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN"
        or CHAT_ID == "YOUR_TELEGRAM_CHAT_ID"
    ):
        logging.info(
            "[TG MOCK ALERT]\n%s",
            text
        )
        return False

    url = (
        f"https://api.telegram.org/"
        f"bot{BOT_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }

    try:

        async with session.post(
            url,
            json=payload,
            timeout=10
        ) as resp:

            if resp.status != 200:

                logging.error(
                    "Telegram HTTP %s: %s",
                    resp.status,
                    await resp.text()
                )

                return False

            return True

    except Exception as e:

        logging.error(
            "Telegram error: %s",
            e
        )

        return False


# ============================================================
#                 STARTUP MESSAGE
# ============================================================

async def send_startup_message(session):

    message = (
        "🟢 <b>Crypto_pulse запущен</b>\n\n"
        f"📈 Поиск импульса: <b>±{PRICE_CHANGE_PCT:.1f}%</b>\n"
        f"⏱ Окно поиска: <b>{LOOKBACK_MINUTES} минут</b>\n"
        f"🔄 Проверка рынка: <b>{PRICE_CHECK_INTERVAL} сек</b>\n"
        f"📊 RVOL: <b>информационный, НЕ фильтр</b>\n"
        f"🔥 Short RVOL: <b>5m</b>\n\n"
        "🚀 <i>Мониторинг рынка начат.</i>"
    )

    await send_telegram_alert(
        session,
        message
    )


# ============================================================
#                 BINGX TICKERS
# ============================================================

async def get_market_tickers(session):

    url = (
        "https://open-api.bingx.com/"
        "openApi/swap/v2/quote/ticker"
    )

    try:

        async with session.get(
            url,
            timeout=10
        ) as resp:

            if resp.status != 200:
                return []

            data = await resp.json()

            if data.get("code") != 0:
                return []

            result = []

            for item in data.get("data", []):

                symbol = normalize_symbol(
                    item.get("symbol")
                )

                if not is_crypto_usdt_symbol(symbol):
                    continue

                try:

                    price = float(
                        item.get(
                            "lastPrice",
                            0
                        )
                    )

                    quote_volume = float(
                        item.get(
                            "quoteVolume",
                            0
                        )
                    )

                except (
                    TypeError,
                    ValueError
                ):
                    continue

                if price <= 0:
                    continue

                if quote_volume < MIN_24H_VOLUME_USDT:
                    continue

                result.append(
                    (
                        symbol,
                        price,
                        quote_volume
                    )
                )

            return result

    except Exception as e:

        logging.warning(
            "❌ BingX ticker error: %s",
            e
        )

        return []


# ============================================================
#                 BINGX KLINES
# ============================================================

async def get_klines(
    session,
    symbol,
    interval,
    limit=21
):

    url = (
        "https://open-api.bingx.com/"
        "openApi/swap/v3/quote/klines"
    )

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }

    try:

        async with session.get(
            url,
            params=params,
            timeout=8
        ) as resp:

            if resp.status != 200:
                return []

            data = await resp.json()

            if data.get("code") != 0:
                return []

            candles = data.get(
                "data",
                []
            )

            if not isinstance(
                candles,
                list
            ):
                return []

            if len(candles) < 3:
                return []

            candles = sorted(
                candles,
                key=lambda x: parse_kline(x)[0]
            )

            return candles

    except Exception:

        return []


# ============================================================
#                  HOURLY RVOL
# ============================================================

async def fetch_hourly_rvol(
    session,
    symbol
):

    klines = await get_klines(
        session,
        symbol,
        "1h",
        21
    )

    if len(klines) < 5:
        return 1.0

    volumes = []

    for k in klines:

        _, _, _, _, close, volume = parse_kline(k)

        if close <= 0 or volume <= 0:
            continue

        usdt_volume = (
            volume * close
        )

        volumes.append(
            usdt_volume
        )

    if len(volumes) < 5:
        return 1.0

    current_volume = volumes[-1]

    previous = volumes[:-1]

    avg_volume = (
        sum(previous)
        / len(previous)
    )

    if avg_volume <= 0:
        return 1.0

    # Экстраполяция текущей часовой свечи
    now = time.time()

    candle_start = (
        int(now) // 3600
    ) * 3600

    elapsed = max(
        1,
        now - candle_start
    )

    fraction = (
        elapsed / 3600.0
    )

    fraction = max(
        0.02,
        min(fraction, 1.0)
    )

    projected = (
        current_volume
        / fraction
    )

    return projected / avg_volume


# ============================================================
#                 SHORT RVOL 5M
# ============================================================

async def fetch_short_rvol(
    session,
    symbol
):

    klines = await get_klines(
        session,
        symbol,
        SHORT_RVOL_INTERVAL,
        SHORT_RVOL_LOOKBACK + 6
    )

    if len(klines) < 5:
        return 1.0

    volumes = []

    for k in klines:

        _, _, _, _, close, volume = parse_kline(k)

        if close <= 0 or volume <= 0:
            continue

        usdt_volume = (
            volume * close
        )

        volumes.append(
            usdt_volume
        )

    if len(volumes) < 5:
        return 1.0

    # Последние несколько 5m свечей
    recent_count = min(
        SHORT_RVOL_RECENT_COUNT,
        len(volumes)
    )

    recent = volumes[
        -recent_count:
    ]

    # Историческая база
    historical_end = (
        len(volumes)
        - recent_count
    )

    historical_start = max(
        0,
        historical_end
        - SHORT_RVOL_LOOKBACK
    )

    historical = volumes[
        historical_start:
        historical_end
    ]

    if not historical:
        return 1.0

    avg_volume = (
        sum(historical)
        / len(historical)
    )

    if avg_volume <= 0:
        return 1.0

    recent_average = (
        sum(recent)
        / len(recent)
    )

    return (
        recent_average
        / avg_volume
    )


# ============================================================
#                 PRICE HISTORY
# ============================================================

def add_price_sample(
    symbol,
    timestamp,
    price
):

    history = PRICE_HISTORY.get(
        symbol
    )

    if history is None:

        history = deque(
            maxlen=MAX_HISTORY_POINTS
        )

        PRICE_HISTORY[symbol] = history

    history.append(
        (
            timestamp,
            price
        )
    )


def get_oldest_price_in_window(
    history,
    target_timestamp
):

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

    return (
        oldest_t,
        oldest_p
    )


# ============================================================
#                 ALERT LOGIC
# ============================================================

def check_alert_conditions(
    symbol,
    change_pct,
    now_timestamp
):

    state = ALERT_STATE.get(
        symbol
    )

    if not state:
        return True, 1

    prev_change = state[
        "change_pct"
    ]

    prev_time = state[
        "time"
    ]

    last_step = state.get(
        "step",
        1
    )

    # Смена направления
    if (
        prev_change > 0
        and change_pct < 0
    ) or (
        prev_change < 0
        and change_pct > 0
    ):

        return True, 1

    current_step = int(
        abs(change_pct)
        // PRICE_CHANGE_PCT
    )

    # Новый уровень:
    # 4 → 8 → 12 → 16...
    if current_step > last_step:

        return True, current_step

    # Кулдаун
    if (
        now_timestamp - prev_time
        >= ALERT_COOLDOWN_SECONDS
    ):

        return True, current_step

    return False, last_step


# ============================================================
#                 PROCESS TICKERS
# ============================================================

async def process_tickers(
    session,
    tickers
):

    now_timestamp = time.time()

    target_timestamp = (
        now_timestamp
        - LOOKBACK_SECONDS
    )

    alerts = 0
    ready = 0

    for (
        symbol,
        current_price,
        quote_volume
    ) in tickers:

        add_price_sample(
            symbol,
            now_timestamp,
            current_price
        )

        history = PRICE_HISTORY[
            symbol
        ]

        old_t, old_price = (
            get_oldest_price_in_window(
                history,
                target_timestamp
            )
        )

        # Ещё не накопилась история
        if (
            old_price is None
            or old_price <= 0
        ):

            continue

        actual_minutes = max(
            1,
            int(
                (
                    now_timestamp
                    - old_t
                ) / 60
            )
        )

        ready += 1

        change_pct = (
            (
                current_price
                - old_price
            )
            / old_price
        ) * 100.0

        # Главный триггер — ТОЛЬКО цена.
        # RVOL здесь НЕ участвует.
        if (
            abs(change_pct)
            < PRICE_CHANGE_PCT
        ):

            continue

        (
            should_alert,
            step_level
        ) = check_alert_conditions(
            symbol,
            change_pct,
            now_timestamp
        )

        if not should_alert:
            continue

        # ====================================================
        # RVOL считаем ТОЛЬКО после обнаружения импульса
        # ====================================================

        hourly_rvol = (
            await fetch_hourly_rvol(
                session,
                symbol
            )
        )

        short_rvol = (
            await fetch_short_rvol(
                session,
                symbol
            )
        )

        # Направление
        if change_pct > 0:

            direction = "UP"
            emoji = "🚀"
            sign = "+"

        else:

            direction = "DOWN"
            emoji = "🔻"
            sign = ""

        clean_symbol = (
            symbol.replace(
                "-",
                ""
            )
        )

        # Оценка короткого RVOL
        if short_rvol >= 5:

            rvol_comment = (
                "🔥 ОЧЕНЬ сильный краткосрочный объём"
            )

        elif short_rvol >= 3:

            rvol_comment = (
                "⚡ Сильный краткосрочный объём"
            )

        elif short_rvol >= 2:

            rvol_comment = (
                "📈 Повышенный краткосрочный объём"
            )

        elif short_rvol >= 1:

            rvol_comment = (
                "📊 Объём выше/около нормы"
            )

        else:

            rvol_comment = (
                "💤 Краткосрочный объём слабый"
            )

        message = (
            f"{emoji} <b>{clean_symbol}</b>\n\n"

            f"📈 <b>Изменение "
            f"за ~{actual_minutes}м:</b> "
            f"{sign}{change_pct:.2f}%\n"

            f"📊 <b>RVOL 1H:</b> "
            f"{hourly_rvol:.2f}x\n"

            f"🔥 <b>Short RVOL 5m:</b> "
            f"{short_rvol:.2f}x\n"

            f"{rvol_comment}\n\n"

            f"💰 <b>Текущая цена:</b> "
            f"{format_price(current_price)}\n"

            f"⏪ <b>Старая цена:</b> "
            f"{format_price(old_price)}\n"

            f"💵 <b>Объём 24ч:</b> "
            f"${quote_volume:,.0f}\n\n"

            f"⚡ <i>Импульс достиг "
            f"{PRICE_CHANGE_PCT:.2f}% "
            f"(Шаг {step_level})</i>"
        )

        await send_telegram_alert(
            session,
            message
        )

        ALERT_STATE[symbol] = {
            "time": now_timestamp,
            "change_pct": change_pct,
            "direction": direction,
            "step": step_level
        }

        alerts += 1

        logging.info(
            "🚀 СИГНАЛ | %s | %s%.2f%% "
            "за %dm | RVOL 1H %.2fx | "
            "Short RVOL %.2fx",
            clean_symbol,
            sign,
            change_pct,
            actual_minutes,
            hourly_rvol,
            short_rvol
        )

    return (
        ready,
        alerts
    )


# ============================================================
#                     MAIN LOOP
# ============================================================

async def main_loop():

    global ACTIVE_SYMBOLS

    timeout = aiohttp.ClientTimeout(
        total=15
    )

    async with aiohttp.ClientSession(
        timeout=timeout
    ) as session:

        logging.info(
            "🚀 CRYPTO_PULSE ЗАПУЩЕН"
        )

        # ====================================================
        # ПРИВЕТСТВИЕ ПОСЛЕ ДЕПЛОЯ
        # ====================================================

        await send_startup_message(
            session
        )

        while True:

            started = time.time()

            tickers = (
                await get_market_tickers(
                    session
                )
            )

            if tickers:

                ACTIVE_SYMBOLS = {
                    item[0]
                    for item in tickers
                }

                (
                    ready,
                    alerts
                ) = await process_tickers(
                    session,
                    tickers
                )

                logging.info(
                    "📊 СКАН | пар=%d | "
                    "готово=%d | "
                    "сигналов=%d | "
                    "история=%d",
                    len(tickers),
                    ready,
                    alerts,
                    len(PRICE_HISTORY)
                )

            else:

                logging.warning(
                    "⚠️ Ticker BingX не получен."
                )

            elapsed = (
                time.time()
                - started
            )

            sleep_time = max(
                1,
                PRICE_CHECK_INTERVAL
                - elapsed
            )

            await asyncio.sleep(
                sleep_time
            )


# ============================================================
#                       START
# ============================================================

if __name__ == "__main__":

    flask_thread = threading.Thread(
        target=run_flask,
        daemon=True
    )

    flask_thread.start()

    asyncio.run(
        main_loop()
    )
