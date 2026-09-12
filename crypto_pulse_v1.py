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
PORT = int(os.getenv("PORT", 8081))  # Порт для Render
BINGX_BASE = "https://open-api.bingx.com"

MIN_24H_VOLUME = 1_000_000  # Мин. объем $1M
WATCH_USERS = set()         # ID пользователей
PROCESSED_SIGNALS = {}      # Кэш сигналов {symbol: timestamp}

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

logging.basicConfig(level=logging.INFO)

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def safe_float(val, default=0.0):
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default

# --- ПОЛУЧЕНИЕ ДАННЫХ BINGX ---
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
                
                if not sym.endswith("-USDT"):
                    continue

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

async def get_klines(session, symbol, interval, limit=30):
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

# --- МОДУЛЬ 1: ДЕТЕКЦИЯ ПОЛКИ И ANTI-MM (15M LIVE) ---
def analyze_shelf_and_anti_mm(candles_15m):
    if len(candles_15m) < 8:
        return False, {}

    # 1. Анализ полки накопления (4-7 прошлые свечи)
    pre_shelf = candles_15m[-7:-1]
    shelf_high = max(c["high"] for c in pre_shelf)
    shelf_low = min(c["low"] for c in pre_shelf)
    
    if shelf_low <= 0:
        return False, {}

    shelf_range_pct = ((shelf_high - shelf_low) / shelf_low) * 100
    
    # Полка должна быть узкой (волатильность <= 1.5%)
    if shelf_range_pct > 1.5:
        return False, {}

    # 2. Анализ Live-свечи (пробой в процессе)
    live = candles_15m[-1]
    impulse_pct = ((live["close"] - shelf_high) / shelf_high) * 100

    # Разрешенный диапазон первичного выстрела: +0.8%..+4.0%
    if not (0.8 <= impulse_pct <= 4.0):
        return False, {}

    # 3. ANTI-MM ФИЛЬТРЫ (Разгрузка об лимиты)
    candle_range = live["high"] - live["low"]
    if candle_range > 0:
        upper_wick = live["high"] - max(live["open"], live["close"])
        wick_ratio = upper_wick / candle_range
        # Если верхний фитиль занимает >25% свечи — идет разгрузка ММ
        if wick_ratio > 0.25:
            return False, {}

    # Свеча строго зеленая
    if live["close"] <= live["open"]:
        return False, {}

    return True, {
        "shelf_high": shelf_high,
        "impulse_pct": impulse_pct,
        "live_price": live["close"]
    }

# --- МОДУЛЬ 2: ВАЛИДАЦИЯ 1M-РАДАРА ---
def validate_1m_radar(candles_1m):
    if len(candles_1m) < 10:
        return False, {}

    curr = candles_1m[-1]   # Текущая свеча 1M
    prev = candles_1m[-2]   # Предыдущая свеча 1M (откатная)
    
    volumes_1m = [c["volume"] for c in candles_1m[-10:-2]]
    avg_vol = sum(volumes_1m) / len(volumes_1m) if volumes_1m else 0.0
    if avg_vol <= 0:
        return False, {}

    # На откатной свече объем ДОЛЖЕН УПАСТЬ (продавцы выдохлись)
    prev_rvol = prev["volume"] / avg_vol
    if prev_rvol > 1.3:
        return False, {}  # На откате слишком высокий объем (слив)

    # Текущая свеча: Зеленая + Позитивный RVOL >= 1.5x + Пробой микро-хая
    curr_rvol = curr["volume"] / avg_vol
    is_green = curr["close"] > curr["open"]
    break_micro_high = curr["close"] > prev["high"]

    if is_green and curr_rvol >= 1.5 and break_micro_high:
        return True, {"rvol_1m": curr_rvol}

    return False, {}

# --- ОСНОВНОЙ ЦИКЛ СКАНИРОВАНИЯ ---
async def fast_scanner_loop(session):
    while True:
        try:
            tickers = await get_tickers(session)
            now = time.time()

            # Очистка устаревших сигналов (кэш 1 час)
            for sym in list(PROCESSED_SIGNALS.keys()):
                if now - PROCESSED_SIGNALS[sym] > 3600:
                    del PROCESSED_SIGNALS[sym]

            for symbol in tickers:
                if symbol in PROCESSED_SIGNALS:
                    continue

                # Запрашиваем свечи 15m и 1m
                candles_15m = await get_klines(session, symbol, "15m", limit=15)
                
                shelf_ok, shelf_meta = analyze_shelf_and_anti_mm(candles_15m)
                if not shelf_ok:
                    await asyncio.sleep(0.05)
                    continue

                candles_1m = await get_klines(session, symbol, "1m", limit=15)
                radar_ok, radar_meta = validate_1m_radar(candles_1m)

                if shelf_ok and radar_ok:
                    PROCESSED_SIGNALS[symbol] = now  # Фиксируем отправку
                    clean_sym = symbol.replace("-USDT", "")
                    
                    msg = (
                        f"🚀 **ПАРТИЗАН: ПРОБОЙ ПОЛКИ (SHELF BREAKOUT)**\n\n"
                        f"Монета: **{clean_sym}**\n"
                        f"📊 Выход из полки: **+{shelf_meta['impulse_pct']:.2f}%**\n"
                        f"⚡️ RVOL 1M: **{radar_meta['rvol_1m']:.2f}x**\n"
                        f"🛡 Anti-MM: **Пройден (Фитиль <25%)**\n"
                        f"💰 Цена: **{shelf_meta['live_price']:.6f}**\n\n"
                        f"🔗 [Открыть график](https://bingx.com/ru-ru/futures/forward/{clean_sym}USDT)"
                    )

                    for u_id in WATCH_USERS:
                        try:
                            await bot.send_message(u_id, msg, parse_mode="Markdown", disable_web_page_preview=True)
                        except Exception:
                            pass

                await asyncio.sleep(0.1)

        except Exception as e:
            logging.error(f"Scanner loop error: {e}")

        await asyncio.sleep(5)

# --- AIOGRAM ХЕНДЛЕРЫ ---
@dp.message(Command("start"))
async def cmd_start(msg: types.Message):
    WATCH_USERS.add(msg.from_user.id)
    await msg.answer("⚡️ Сканер «Партизан» переведен на Shelf Breakout + Anti-MM! Ожидаем пробои накопительных полок...")

# --- HEALTH CHECK СЕРВЕР ДЛЯ RENDER ---
async def health(request):
    return web.Response(text="OK", status=200)

async def main():
    async with aiohttp.ClientSession() as session:
        # Настройка сервера фиктивного порта для Render
        app = web.Application()
        app.router.add_get("/", health)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", PORT).start()
        logging.info(f"Health-check server running on port {PORT}")

        # Запуск сканера
        asyncio.create_task(fast_scanner_loop(session))

        await bot.delete_webhook(drop_pending_updates=True)
        try:
            await dp.start_polling(bot)
        finally:
            await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
