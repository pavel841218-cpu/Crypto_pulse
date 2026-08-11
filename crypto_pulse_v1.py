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

# ============================================================
# ВОТ ЗДЕСЬ МЕНЯЕШЬ ПЕРИОД
#
# Доступно:
# "1m"
# "5m"
# "15m"
# "30m"
# "1h"
# "2h"
# "4h"
# ============================================================

PRICE_CHANGE_TIMEFRAME = "1h"


# ============================================================
# ВОТ ЗДЕСЬ МЕНЯЕШЬ ПРОЦЕНТ
#
# Например:
# 2.0 = движение от 2%
# 3.0 = движение от 3%
# 4.0 = движение от 4%
# 5.0 = движение от 5%
# ============================================================

PRICE_CHANGE_PCT = 4.0


# Как часто опрашивать рынок
# 30 секунд — хороший вариант для Render
PRICE_CHECK_INTERVAL = 30


# Минимальный объём монеты за 24 часа.
# Это отсеивает совсем мёртвые инструменты.
MIN_24H_VOLUME_USDT = 2_000_000


# Защита от повторных сообщений
ALERT_COOLDOWN_SECONDS = 1800  # 30 минут


# После того как импульс упал ниже этого уровня,
# монета снова считается готовой к новому сигналу.
REARM_PCT = PRICE_CHANGE_PCT * 0.50


# Подробный лог каждого сканирования.
# False = Render не будет забит тысячами строк.
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
    raise ValueError(
        "PRICE_CHANGE_PCT должен быть больше 0"
    )


PERIOD_SECONDS = SUPPORTED_TIMEFRAMES[
    PRICE_CHANGE_TIMEFRAME
]


# Сколько точек истории держим в памяти.
#
# Например:
# 1h / 30 сек ≈ 120 точек
# 4h / 30 сек ≈ 480 точек
#
# + запас
MAX_HISTORY_POINTS = int(
    PERIOD_SECONDS / PRICE_CHECK_INTERVAL
) + 20


# ============================================================
#          ФИЛЬТР НЕ-КРИПТОВЫХ ИНСТРУМЕНТОВ
# ============================================================

# Именно из-за подобных инструментов у тебя раньше появлялись
# NCS..., NCCO..., индексы и прочий мусор.
#
# Мы хотим именно крипто-USDT фьючерсы.

NON_CRYPTO_PREFIXES = (
    "NCS",
    "NCCO",
    "SP500",
    "US30",
    "US100",
    "NAS100",
    "GER40",
    "UK100",
    "JPN225",
    "GOLD",
    "SILVER",
    "BRENT",
    "WTI",
    "OIL",
    "XAU",
    "XAG",
    "XTI",
    "XBR",
    "EUR",
    "GBP",
    "JPY",
    "AUD",
    "CAD",
    "CHF",
    "HK",
    "DXY",
)


EXCLUDED_SYMBOLS = {
    "USDC-USDT",
    "FDUSD-USDT",
    "USD1-USDT",
}


# ============================================================
#                      MEMORY
# ============================================================

# Для каждой монеты храним:
#
# symbol -> [(timestamp, price), ...]
#
# Например:
#
# HOME-USDT:
# 22:00  0.00980
# 22:00:30 0.00982
# 22:01 0.00985
# ...
#
# Благодаря этому мы можем сравнивать цену ровно
# с ценой N минут/часов назад.

PRICE_HISTORY = {}


# Состояние последнего сигнала:
#
# symbol -> {
#     time,
#     change_pct,
#     direction
# }
#
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
#                FLASK KEEP-ALIVE
# ============================================================

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
    app.run(
        host="0.0.0.0",
        port=PORT
    )


# ============================================================
#                 SYMBOL NORMALIZATION
# ============================================================

def normalize_symbol(raw):
    """
    Приводит:

    BTCUSDT
    BTC-USDT

    к:

    BTC-USDT
    """

    symbol = str(
        raw or ""
    ).strip().upper()

    if not symbol:
        return None

    if symbol.endswith("-USDT"):
        return symbol

    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}-USDT"

    return None


def is_crypto_usdt_symbol(symbol):
    """
    Оставляем только нормальные крипто-USDT пары.
    """

    if not symbol:
        return False

    if not symbol.endswith("-USDT"):
        return False

    if symbol in EXCLUDED_SYMBOLS:
        return False

    base = symbol[:-5]

    # Отсекаем индексы/товары/FX
    if any(
        base.startswith(prefix)
        for prefix in NON_CRYPTO_PREFIXES
    ):
        return False

    # Отсекаем странные составные инструменты
    if "(" in base:
        return False

    if ")" in base:
        return False

    if "/" in base:
        return False

    return True


# ============================================================
#                 TELEGRAM
# ============================================================

async def send_telegram_alert(
    session,
    text
):
    """
    Отправка сообщения в Telegram.
    """

    if (
        BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN"
        or
        CHAT_ID == "YOUR_TELEGRAM_CHAT_ID"
    ):
        logging.info(
            "[TG MOCK ALERT]\n%s",
            text
        )
        return

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

    except Exception as e:

        logging.error(
            "Telegram error: %s",
            e
        )


# ============================================================
#                  BINGX TICKER
# ============================================================

async def get_market_tickers(session):
    """
    Получаем ВСЕ текущие тикеры одним запросом.

    Это намного лучше, чем делать сотни запросов
    за свечами для каждой монеты.
    """

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

                logging.warning(
                    "❌ BingX ticker HTTP %s",
                    resp.status
                )

                return []

            data = await resp.json()

            if data.get("code") != 0:

                logging.warning(
                    "❌ BingX ticker code=%s msg=%s",
                    data.get("code"),
                    data.get("msg")
                )

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
#                  PRICE HISTORY
# ============================================================

def add_price_sample(
    symbol,
    timestamp,
    price
):
    """
    Добавляем текущую цену в историю.
    """

    history = PRICE_HISTORY.get(symbol)

    if history is None:

        history = deque(
            maxlen=MAX_HISTORY_POINTS
        )

        PRICE_HISTORY[symbol] = history

    # Защита от повторного timestamp
    if history:

        if timestamp <= history[-1][0]:
            return

    history.append(
        (
            timestamp,
            price
        )
    )


# ============================================================
#             ПОЛУЧЕНИЕ ЦЕНЫ В ПРОШЛОМ
# ============================================================

def price_at(
    history,
    target_timestamp
):
    """
    Находим цену максимально точно в нужный момент.

    Если между двумя сохранёнными точками находится
    нужный момент — делаем линейную интерполяцию.

    Благодаря этому:

    1h = действительно примерно последние 60 минут,

    а не "текущая свеча против предыдущей свечи".
    """

    if not history:
        return None

    # Истории ещё недостаточно
    if target_timestamp < history[0][0]:
        return None

    # Если target уже ближе к последней точке
    if target_timestamp >= history[-1][0]:
        return history[-1][1]

    points = list(history)

    prev_t, prev_p = points[0]

    for curr_t, curr_p in points[1:]:

        if curr_t >= target_timestamp:

            if curr_t == prev_t:
                return curr_p

            ratio = (
                (target_timestamp - prev_t)
                /
                (curr_t - prev_t)
            )

            return (
                prev_p
                +
                (curr_p - prev_p)
                * ratio
            )

        prev_t = curr_t
        prev_p = curr_p

    return None


# ============================================================
#                   FORMAT PRICE
# ============================================================

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
#                 ALERT CONTROL
# ============================================================

def can_alert(
    symbol,
    change_pct,
    now_timestamp
):
    """
    Защита от спама.

    После сигнала монета не будет сыпать сообщениями
    каждую минуту.

    Чтобы получить новый сигнал, движение сначала
    должно вернуться ниже REARM_PCT.
    """

    state = ALERT_STATE.get(symbol)

    if not state:
        return True

    previous_change = state["change_pct"]

    # Если направление изменилось,
    # разрешаем новый сигнал.
    if (
        previous_change > 0
        and
        change_pct < 0
    ):
        return True

    if (
        previous_change < 0
        and
        change_pct > 0
    ):
        return True

    # Если импульс практически исчез,
    # снова вооружаем монету.
    if abs(change_pct) <= REARM_PCT:
        return True

    # Дополнительная страховка
    if (
        now_timestamp
        -
        state["time"]
        >= ALERT_COOLDOWN_SECONDS
    ):
        return True

    return False


# ============================================================
#                 PROCESS MARKET
# ============================================================

async def process_tickers(
    session,
    tickers
):
    """
    Главная логика импульса.
    """

    now_timestamp = time.time()

    # Например при 1h:
    #
    # сейчас = 22:30
    # target = 21:30
    #
    target_timestamp = (
        now_timestamp
        -
        PERIOD_SECONDS
    )

    alerts = 0
    ready = 0

    strongest_move = None

    for (
        symbol,
        current_price,
        quote_volume
    ) in tickers:

        # ----------------------------------------------------
        # Записываем текущую цену
        # ----------------------------------------------------

        add_price_sample(
            symbol,
            now_timestamp,
            current_price
        )

        history = PRICE_HISTORY[symbol]

        # ----------------------------------------------------
        # Ищем цену N времени назад
        # ----------------------------------------------------

        old_price = price_at(
            history,
            target_timestamp
        )

        # Истории пока недостаточно
        if old_price is None:
            continue

        if old_price <= 0:
            continue

        ready += 1

        # ----------------------------------------------------
        # РАСЧЁТ ИЗМЕНЕНИЯ
        # ----------------------------------------------------

        change_pct = (
            (
                current_price
                -
                old_price
            )
            /
            old_price
        ) * 100.0

        # Запоминаем самый сильный импульс
        if (
            strongest_move is None
            or
            abs(change_pct)
            >
            abs(strongest_move[1])
        ):

            strongest_move = (
                symbol,
                change_pct
            )

        # ----------------------------------------------------
        # Порог не достигнут
        # ----------------------------------------------------

        if abs(change_pct) < PRICE_CHANGE_PCT:
            continue

        # ----------------------------------------------------
        # Направление
        # ----------------------------------------------------

        if change_pct > 0:

            direction = "UP"

        else:

            direction = "DOWN"

        # ----------------------------------------------------
        # Проверка антиспама
        # ----------------------------------------------------

        if not can_alert(
            symbol,
            change_pct,
            now_timestamp
        ):
            continue

        # ----------------------------------------------------
        # Эмодзи
        # ----------------------------------------------------

        if change_pct > 0:

            emoji = "🚀"

        else:

            emoji = "🔻"

        if change_pct > 0:

            sign = "+"

        else:

            sign = ""

        # ----------------------------------------------------
        # Telegram сообщение
        # ----------------------------------------------------

        clean_symbol = symbol.replace(
            "-",
            ""
        )

        message = (
            f"{emoji} "
            f"<b>{clean_symbol}</b>\n\n"

            f"📈 <b>Изменение за "
            f"{PRICE_CHANGE_TIMEFRAME}:</b> "
            f"{sign}{change_pct:.2f}%\n"

            f"💰 <b>Текущая цена:</b> "
            f"{format_price(current_price)}\n"

            f"⏪ <b>Цена "
            f"{PRICE_CHANGE_TIMEFRAME} назад:</b> "
            f"{format_price(old_price)}\n"

            f"💵 <b>Объём 24ч:</b> "
            f"${quote_volume:,.0f}\n\n"

            f"⚡ <i>Импульс достиг "
            f"порога {PRICE_CHANGE_PCT:.2f}%</i>"
        )

        # ----------------------------------------------------
        # Отправляем
        # ----------------------------------------------------

        await send_telegram_alert(
            session,
            message
        )

        ALERT_STATE[symbol] = {
            "time": now_timestamp,
            "change_pct": change_pct,
            "direction": direction
        }

        alerts += 1

        logging.info(
            "🚀 СИГНАЛ | %s | %s%.2f%% за %s",
            clean_symbol,
            sign,
            change_pct,
            PRICE_CHANGE_TIMEFRAME
        )

    # --------------------------------------------------------
    # Лог
    # --------------------------------------------------------

    if LOG_EVERY_SCAN:

        if strongest_move:

            logging.info(
                "SCAN | готово=%d | "
                "сильнейший=%s %.2f%%",
                ready,
                strongest_move[0],
                strongest_move[1]
            )

    return ready, alerts


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
            "🚀 ======================================="
        )

        logging.info(
            "🚀 CONSOLIDATION HUNTER ЗАПУЩЕН"
        )

        logging.info(
            "⏱ Период импульса: %s",
            PRICE_CHANGE_TIMEFRAME
        )

        logging.info(
            "📈 Порог импульса: %.2f%%",
            PRICE_CHANGE_PCT
        )

        logging.info(
            "🔄 Проверка каждые: %s сек",
            PRICE_CHECK_INTERVAL
        )

        logging.info(
            "💵 Минимальный объём 24ч: $%s",
            f"{MIN_24H_VOLUME_USDT:,.0f}"
        )

        logging.info(
            "🧠 Максимум точек истории на монету: %d",
            MAX_HISTORY_POINTS
        )

        logging.info(
            "⚠️ После перезапуска бот должен "
            "накопить историю периода %s",
            PRICE_CHANGE_TIMEFRAME
        )

        logging.info(
            "🚀 ======================================="
        )

        while True:

            started = time.time()

            # ------------------------------------------------
            # Получаем рынок одним запросом
            # ------------------------------------------------

            tickers = await get_market_tickers(
                session
            )

            if tickers:

                ACTIVE_SYMBOLS = {
                    item[0]
                    for item in tickers
                }

                ready, alerts = await process_tickers(
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
                    "⚠️ Ticker BingX не получен. "
                    "Следующая попытка через %s сек.",
                    PRICE_CHECK_INTERVAL
                )

            # ------------------------------------------------
            # Не запускаем цикл быстрее заданного интервала
            # ------------------------------------------------

            elapsed = (
                time.time()
                -
                started
            )

            sleep_time = max(
                1,
                PRICE_CHECK_INTERVAL
                -
                elapsed
            )

            await asyncio.sleep(
                sleep_time
            )


# ============================================================
#                       START
# ============================================================

if __name__ == "__main__":

    # Render Keep Alive
    flask_thread = threading.Thread(
        target=run_flask,
        daemon=True
    )

    flask_thread.start()

    # Запуск бота
    asyncio.run(
        main_loop()
    )
