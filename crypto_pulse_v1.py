import os
import time
import logging
import asyncio
import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiohttp import web

# --- НАСТРОЙКИ ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", 8081))  # Отдельный порт для Render
BINGX_BASE = "https://open-api.bingx.com"

MIN_24H_VOLUME = 1_000_000  # Мин. объем 1M $
WATCH_USERS = set()         # ID пользователей
PROCESSED_SIGNALS = {}      # Кэш сигналов {symbol: timestamp} для защиты от спама

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

logging.basicConfig(level=logging.INFO)

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def safe_float(val, default=0.0):
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default

def calculate_ema(prices, period):
    if len(prices) < period:
        return []
    k = 2 / (period + 1)
    ema = [sum(prices[:period]) / period]
    for p in prices[period:]:
        ema.append(p * k + ema[-1] * (1 - k))
    return ema

# --- ВАЛИДАЦИЯ И ПОЛУЧЕНИЕ ТИКЕРОВ ---
async def get_tickers(session):
    url = f"{BINGX_BASE}/openApi/swap/v2/quote/ticker"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 429:
                logging.warning("Превышен лимит API (429)! Пауза 15 сек...")
                await asyncio.sleep(15)
                return {}
            data = await resp.json()
            if data.get("code") != 0:
                return {}
            result = {}
            for item in data.get("data", []):
                sym = str(item.get("symbol", "")).upper()
                
                # Исключаем не-USDT пары
                if not sym.endswith("-USDT"):
                    continue

                # Исключаем индексы, активы фонды, металлы и спец-префиксы BingX
                if sym.startswith(("NC", "INDEX")) or any(x in sym for x in ["FOOTBALL", "NASDAQ", "SP500", "DOWJONES", "XAU", "XAG", "_"]):
                    continue

                vol = safe_float(item.get("quoteVolume"))
                price = safe_float(item.get("lastPrice"))
                if vol >= MIN_24H_VOLUME and price > 0:
                    result[sym] = {"volume": vol, "price": price}
            return result
    except Exception as e:
        logging.error(f"Tickers error: {e}")
        return {}

async def get_klines(session, symbol, interval, limit=50):
    url = f"{BINGX_BASE}/openApi/swap/v3/quote/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 429:
                await asyncio.sleep(5)
                return []
            data = await resp.json()
            if data.get("code") != 0 or not data.get("data"):
                return []
            
            candles = []
            for k in data["data"]:
                candles.append({
                    "open": safe_float(k.get("open")),
                    "high": safe_float(k.get("high")),
                    "low": safe_float(k.get("low")),
                    "close": safe_float(k.get("close")),
                    "volume": safe_float(k.get("volume"))
                })
            return candles[::-1]  # От старых к новым
    except Exception as e:
        logging.error(f"Klines error {symbol} ({interval}): {e}")
        return []

# --- ДЕТЕКЦИЯ СТАРТА ИМПУЛЬСА (1M + 15M) ---
def analyze_fast_pump(candles_1m, candles_15m):
    # 1. Тренд на 15m (Бычий веер EMA 7 > 25 > 50)
    if len(candles_15m) < 50:
        return False, {}

    closes_15m = [c["close"] for c in candles_15m]
    e7_15 = calculate_ema(closes_15m, 7)[-1]
    e25_15 = calculate_ema(closes_15m, 25)[-1]
    e50_15 = calculate_ema(closes_15m, 50)[-1]

    if not (e7_15 > e25_15 > e50_15):
        return False, {}

    # 2. Быстрый импульс на 1m
    if len(candles_1m) < 20:
        return False, {}

    last_1m = candles_1m[-1]
    prev_1m = candles_1m[-2]

    avg_vol_1m = sum(c["volume"] for c in candles_1m[-16:-1]) / 15.0
    rvol_1m = last_1m["volume"] / avg_vol_1m if avg_vol_1m > 0 else 0.0

    candle_growth_1m = ((last_1m["close"] - last_1m["open"]) / last_1m["open"]) * 100

    # ФИЛЬТР ВХОДА В НАЧАЛЕ ПАМПА:
    # - RVOL 1m >= 3.0x
    # - Рост 1m свечи строго от +0.2% до +0.8% (самый старт)
    # - Цена пробивает хай предыдущей минутки
    if rvol_1m >= 3.0 and 0.2 <= candle_growth_1m <= 0.8 and last_1m["close"] > prev_1m["high"]:
        return True, {
            "rvol": rvol_1m,
            "growth": candle_growth_1m,
            "price": last_1m["close"]
        }

    return False, {}

# --- ОСНОВНОЙ ЦИКЛ СКАНИРОВАНИЯ С ЗАЩИТОЙ ОТ СПАМА ---
async def fast_scanner_loop(session):
    while True:
        try:
            tickers = await get_tickers(session)
            now = time.time()

            # Очистка устаревших сигналов из кэша (старше 1 часа / 3600 сек)
            for sym in list(PROCESSED_SIGNALS.keys()):
                if now - PROCESSED_SIGNALS[sym] > 3600:
                    del PROCESSED_SIGNALS[sym]

            for symbol in tickers:
                # Если по этой монете уже шёл сигнал за последний час — пропускаем
                if symbol in PROCESSED_SIGNALS:
                    continue

                candles_15m = await get_klines(session, symbol, "15m", limit=50)
                candles_1m = await get_klines(session, symbol, "1m", limit=20)

                triggered, meta = analyze_fast_pump(candles_1m, candles_15m)

                if triggered:
                    PROCESSED_SIGNALS[symbol] = now  # Запоминаем отправку
                    clean_sym = symbol.replace("-USDT", "")
                    msg = (
                        f"⚡️ **СКАНИРОВАНИЕ 1M: СТАРТ ИМПУЛЬСА!**\n\n"
                        f"Монета: **{clean_sym}**\n"
                        f"📊 RVOL 1m: **{meta['rvol']:.2f}x**\n"
                        f"📈 Рост 1m свечи: **+{meta['growth']:.2f}%** (Самое начало!)\n"
                        f"💰 Цена: **{meta['price']:.6f}**\n\n"
                        f"🔗 [График](https://bingx.com/ru-ru/futures/forward/{clean_sym}USDT)"
                    )

                    for u_id in WATCH_USERS:
                        try:
                            await bot.send_message(u_id, msg, parse_mode="Markdown", disable_web_page_preview=True)
                        except Exception:
                            pass

                # Пауза 0.15с между запросами по тикерам для защиты IP (Rate Limits)
                await asyncio.sleep(0.15)

        except aiohttp.ClientResponseError as e:
            if e.status == 429:
                logging.warning("Превышен лимит API BingX! Ожидание 30 секунд...")
                await asyncio.sleep(30)
        except Exception as e:
            logging.error(f"Fast scanner error: {e}")

        # Пауза 10 секунд перед новым кругом сканирования
        await asyncio.sleep(10)

# --- AIOGRAM ХЕНДЛЕРЫ ---
@dp.message(Command("start"))
async def cmd_start(msg: types.Message):
    WATCH_USERS.add(msg.from_user.id)
    await msg.answer("⚡️ Сканер 1m-пампов запущен! Ждем точечные импульсы...")

# --- СЕРВЕР И HEALTH CHECK ---
async def health(request):
    return web.Response(text="OK", status=200)

async def main():
    async with aiohttp.ClientSession() as session:
        app = web.Application()
        app.router.add_get("/", health)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", PORT).start()

        asyncio.create_task(fast_scanner_loop(session))

        await bot.delete_webhook(drop_pending_updates=True)
        try:
            await dp.start_polling(bot)
        finally:
            await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
