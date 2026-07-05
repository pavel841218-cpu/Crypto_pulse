
# ==========================================
# ЧАСТЬ 1: БИБЛИОТЕКИ И НАСТРОЙКА ЛОГОВ
# ==========================================
import os
import sys
import asyncio
import logging
import httpx
from aiogram import Bot, Dispatcher, types, F
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiohttp import web

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SuppressNetworkErrors(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        if "NetworkError" in msg or "ClientOSError" in msg or "Errno 103" in msg:
            return False
        return True

logging.getLogger().addFilter(SuppressNetworkErrors())

# ==========================================
# ЧАСТЬ 2: ТОКЕНЫ И НАСТРОЙКИ
# ==========================================
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    logging.critical("BOT_TOKEN не найден! Проверьте переменные окружения.")
    sys.exit(1)

CHAT_ID = int(os.getenv("CHAT_ID", "6908511803"))

bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ==========================================
# ЧАСТЬ 3: ПРОФИЛЬ И МЕНЮ
# ==========================================
USER_PROFILE = {
    "exchange": "bingx",
    "alert_percent": 4.0,
    "check_interval": 60,
    "min_price": 0.001,
    "max_price": 100000.0,
}

MANUAL_ALLOWED = set()
MANUAL_BLOCKED = set()
price_history = {}

class BotStates(StatesGroup):
    waiting_for_percent = State()
    waiting_for_time = State()
    waiting_for_add_coin = State()
    waiting_for_del_coin = State()

def get_main_menu():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    ex_names = {"binance": "Binance 🔸", "bitget": "Bitget 🛡️", "bingx": "BingX 💠"}
    current_ex = ex_names.get(USER_PROFILE["exchange"], USER_PROFILE["exchange"].upper())
    t_min = USER_PROFILE["check_interval"] // 60
    keyboard.inline_keyboard.extend([
        [InlineKeyboardButton(text=f"🏦 Биржа: {current_ex}", callback_data="m_exchange")],
        [InlineKeyboardButton(text=f"📈 Порог: {USER_PROFILE['alert_percent']}%", callback_data="m_percent")],
        [InlineKeyboardButton(text=f"⏳ Таймфрейм: {t_min} мин", callback_data="m_time")],
        [InlineKeyboardButton(text="➕ Добавить монету", callback_data="coin_add")],
        [InlineKeyboardButton(text="❌ Удалить монету", callback_data="coin_del")],
        [InlineKeyboardButton(text="🔄 Обновить панель", callback_data="m_refresh")]
    ])
    return keyboard

def get_exchange_kb():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for code, text in [("binance", "Binance 🔸"), ("bitget", "Bitget 🛡️"), ("bingx", "BingX 💠")]:
        display = f"✅ {text}" if USER_PROFILE["exchange"] == code else text
        keyboard.inline_keyboard.append([InlineKeyboardButton(text=display, callback_data=f"set_ex_{code}")])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="m_main")])
    return keyboard

def get_percent_kb():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    percents = [1.0, 2.0, 3.0, 4.0, 5.0, 7.0, 10.0, 15.0, 20.0]
    row = []
    for p in percents:
        display = f"✅ {p}%" if USER_PROFILE["alert_percent"] == p else f"{p}%"
        row.append(InlineKeyboardButton(text=display, callback_data=f"set_pct_{p}"))
        if len(row) == 3:
            keyboard.inline_keyboard.append(row)
            row = []
    if row:
        keyboard.inline_keyboard.append(row)
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="✏️ Ввести свой %", callback_data="inp_pct")])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="m_main")])
    return keyboard

def get_time_kb():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    intervals = [1, 5, 10, 15, 30, 60, 240]
    current_min = USER_PROFILE["check_interval"] // 60
    row = []
    for i in intervals:
        display = f"✅ {i}м" if current_min == i else f"{i}м"
        row.append(InlineKeyboardButton(text=display, callback_data=f"set_t_{i}"))
        if len(row) == 3:
            keyboard.inline_keyboard.append(row)
            row = []
    if row:
        keyboard.inline_keyboard.append(row)
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="✏️ Ввести своё время (мин)", callback_data="inp_t")])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="m_main")])
    return keyboard

def make_profile_text():
    ex_names = {"binance": "BINANCE 🔸", "bitget": "BITGET 🛡️", "bingx": "BINGX 💠"}
    t_min = USER_PROFILE["check_interval"] // 60
    allowed_str = ", ".join(MANUAL_ALLOWED) if MANUAL_ALLOWED else "Нет"
    blocked_str = ", ".join(MANUAL_BLOCKED) if MANUAL_BLOCKED else "Нет"
    return (
        f"⚙️ <b>Панель управления Crypto Pulse 1.0</b>\n\n"
        f"🏦 Активная биржа по умолчанию: <b>{ex_names.get(USER_PROFILE['exchange'])}</b>\n"
        f"📈 Trigger изменения: <b>{USER_PROFILE['alert_percent']}%</b>\n"
        f"⏳ Интервал проверки рынка: <b>{t_min} мин.</b>\n"
        f"🎯 Фильтр базовой цены: <b>{USER_PROFILE['min_price']} - {USER_PROFILE['max_price']} USDT</b>\n\n"
        f"➕ Белый список (ручные монеты): <code>{allowed_str}</code>\n"
        f"❌ Черный список (удаленные монеты): <code>{blocked_str}</code>\n\n"
        f"Алерт-сообщения приходят ниже и не сбивают эту строку настроек! 👇"
    )

# ==========================================
# ОБРАБОТЧИКИ КОМАНД И КНОПОК
# ==========================================
@dp.message(Command('start'))
async def start_cmd(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(make_profile_text(), parse_mode="HTML", reply_markup=get_main_menu())

@dp.callback_query(F.data == "m_main")
async def back_to_main(callback: types.CallbackQuery, state: FSMContext):
    try:
        await state.clear()
        await callback.answer()
        await callback.message.edit_text(make_profile_text(), parse_mode="HTML", reply_markup=get_main_menu())
    except TelegramBadRequest:
        pass

@dp.callback_query(F.data == "m_refresh")
async def refresh_panel(callback: types.CallbackQuery):
    try:
        await callback.answer("Обновлено!")
        await callback.message.edit_text(make_profile_text(), parse_mode="HTML", reply_markup=get_main_menu())
    except TelegramBadRequest:
        pass

@dp.callback_query(F.data == "m_exchange")
async def m_ex_call(callback: types.CallbackQuery):
    try:
        await callback.answer()
        await callback.message.edit_text("🏦 Выбери фьючерсную биржу из списка по умолчанию:", reply_markup=get_exchange_kb())
    except TelegramBadRequest:
        pass

@dp.callback_query(F.data.startswith("set_ex_"))
async def set_ex(callback: types.CallbackQuery):
    try:
        new_ex = callback.data.split("_")[2]
        USER_PROFILE["exchange"] = new_ex
        price_history.clear()
        await callback.answer(f"Переключено на {new_ex.upper()}!", show_alert=True)
        await callback.message.edit_text(make_profile_text(), parse_mode="HTML", reply_markup=get_main_menu())
    except TelegramBadRequest:
        pass

@dp.callback_query(F.data == "m_percent")
async def m_pct_call(callback: types.CallbackQuery):
    try:
        await callback.answer()
        await callback.message.edit_text("📈 Выбери порог изменения цены:", reply_markup=get_percent_kb())
    except TelegramBadRequest:
        pass

@dp.callback_query(F.data.startswith("set_pct_"))
async def set_pct(callback: types.CallbackQuery):
    try:
        USER_PROFILE["alert_percent"] = float(callback.data.split("_")[2])
        await callback.answer("Процент обновлен!")
        await callback.message.edit_text(make_profile_text(), parse_mode="HTML", reply_markup=get_main_menu())
    except TelegramBadRequest:
        pass

@dp.callback_query(F.data == "inp_pct")
async def inp_pct(callback: types.CallbackQuery, state: FSMContext):
    try:
        await callback.answer()
        await state.set_state(BotStates.waiting_for_percent)
        await callback.message.answer("✏️ Введи процент изменения цены (от 1 до 100):")
    except TelegramBadRequest:
        pass

@dp.message(BotStates.waiting_for_percent)
async def proc_custom_pct(message: types.Message, state: FSMContext):
    try:
        val = round(float(message.text.strip().replace(",", ".")), 2)
        if 1.0 <= val <= 100.0:
            USER_PROFILE["alert_percent"] = val
            await state.clear()
            await message.answer(f"✅ Установлен порог в {val}%!", reply_markup=get_main_menu())
        else:
            await message.answer("❌ Введи число от 1 до 100:")
    except ValueError:
        await message.answer("❌ Отправь корректное число цифрами:")

@dp.callback_query(F.data == "m_time")
async def m_time_call(callback: types.CallbackQuery):
    try:
        await callback.answer()
        await callback.message.edit_text("⏳ Выбери интервал сканирования рынка:", reply_markup=get_time_kb())
    except TelegramBadRequest:
        pass

@dp.callback_query(F.data.startswith("set_t_"))
async def set_time(callback: types.CallbackQuery):
    try:
        minutes = int(callback.data.split("_")[2])
        USER_PROFILE["check_interval"] = minutes * 60
        await callback.answer(f"Таймфрейм изменен на {minutes} мин.!")
        await callback.message.edit_text(make_profile_text(), parse_mode="HTML", reply_markup=get_main_menu())
    except TelegramBadRequest:
        pass

@dp.callback_query(F.data == "inp_t")
async def inp_time(callback: types.CallbackQuery, state: FSMContext):
    try:
        await callback.answer()
        await state.set_state(BotStates.waiting_for_time)
        await callback.message.answer("✏️ Введи любое количество минут для таймфрейма:")
    except TelegramBadRequest:
        pass

@dp.message(BotStates.waiting_for_time)
async def proc_custom_time(message: types.Message, state: FSMContext):
    if message.text.isdigit():
        minutes = int(message.text)
        if minutes >= 1:
            USER_PROFILE["check_interval"] = minutes * 60
            await state.clear()
            await message.answer(f"✅ Интервал сканирования обновлен: {minutes} мин.!", reply_markup=get_main_menu())
            return
    await message.answer("❌ Введи корректное целое число минут:")

@dp.callback_query(F.data == "coin_add")
async def coin_add_call(callback: types.CallbackQuery, state: FSMContext):
    try:
        await callback.answer()
        await state.set_state(BotStates.waiting_for_add_coin)
        await callback.message.answer("➕ Напиши тикер монеты, которую нужно принудительно добавить в сканер (например: BTC или SOL):")
    except TelegramBadRequest:
        pass

@dp.message(BotStates.waiting_for_add_coin)
async def proc_coin_add(message: types.Message, state: FSMContext):
    coin = message.text.strip().upper().replace("USDT", "")
    if coin:
        if coin in MANUAL_BLOCKED:
            MANUAL_BLOCKED.remove(coin)
        MANUAL_ALLOWED.add(coin)
        price_history.clear()
        await state.clear()
        await message.answer(f"✅ Монета {coin} добавлена в список исключений сканера!", reply_markup=get_main_menu())
    else:
        await message.answer("Неверный формат ввода.")

@dp.callback_query(F.data == "coin_del")
async def coin_del_call(callback: types.CallbackQuery, state: FSMContext):
    try:
        await callback.answer()
        await state.set_state(BotStates.waiting_for_del_coin)
        await callback.message.answer("❌ Напиши тикер монеты, которую нужно полностью скрыть и удалить из алертов (например: XRP):")
    except TelegramBadRequest:
        pass

@dp.message(BotStates.waiting_for_del_coin)
async def proc_coin_del(message: types.Message, state: FSMContext):
    coin = message.text.strip().upper().replace("USDT", "")
    if coin:
        if coin in MANUAL_ALLOWED:
            MANUAL_ALLOWED.remove(coin)
        MANUAL_BLOCKED.add(coin)
        if f"{coin}USDT" in price_history:
            del price_history[f"{coin}USDT"]
        await state.clear()
        await message.answer(f"❌ Монета {coin} полностью удалена и заблокирована!", reply_markup=get_main_menu())
    else:
        await message.answer("Неверный формат ввода.")

# ==========================================
# РЫНОЧНЫЕ ДАННЫЕ И МОНИТОРИНГ
# ==========================================
async def fetch_market_prices():
    exchange = USER_PROFILE["exchange"]
    filtered = {}
    async with httpx.AsyncClient(timeout=8.0) as client:
        try:
            if exchange == "binance":
                url = "https://fapi.binance.com/fapi/v1/ticker/price"
                response = await client.get(url)
                if response.status_code == 200:
                    for item in response.json():
                        symbol = item['symbol']
                        if symbol.endswith("USDT"):
                            filtered[symbol] = float(item['price'])
            elif exchange == "bitget":
                url = "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES"
                response = await client.get(url)
                if response.status_code == 200:
                    for item in response.json().get("data", []):
                        symbol = item.get('symbol', '')
                        if symbol.endswith("USDT"):
                            filtered[symbol] = float(item.get('lastPr', 0))
            elif exchange == "bingx":
                url = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"
                response = await client.get(url)
                if response.status_code == 200:
                    for item in response.json().get("data", []):
                        symbol = item.get('symbol', '').replace("-", "")
                        if symbol.endswith("USDT"):
                            filtered[symbol] = float(item.get('lastPrice', 0))
            return filtered
        except Exception as e:
            logging.error(f"Ошибка API фьючерсов {exchange.upper()}: {e}")
            return {}

async def drops_monitoring_loop():
    logging.info("Фоновый сканер Crypto Pulse v1 успешно запущен.")
    while True:
        try:
            current_threshold = USER_PROFILE["alert_percent"]
            exchange_label = USER_PROFILE["exchange"].upper()
            current_market = await fetch_market_prices()
            if current_market:
                active_symbols = set(current_market.keys())
                for old_sym in list(price_history.keys()):
                    if old_sym not in active_symbols:
                        del price_history[old_sym]
                for symbol, current_price in current_market.items():
                    clean_ticker = symbol.replace('USDT', '')
                    if clean_ticker in MANUAL_BLOCKED:
                        continue
                    if not (USER_PROFILE["min_price"] <= current_price <= USER_PROFILE["max_price"]):
                        if clean_ticker not in MANUAL_ALLOWED:
                            continue
                    if symbol not in price_history:
                        price_history[symbol] = current_price
                        continue
                    old_price = price_history[symbol]
                    if old_price <= 0:
                        price_history[symbol] = current_price
                        continue
                    percent_change = ((current_price - old_price) / old_price) * 100
                    if abs(percent_change) >= current_threshold:
                        t_min = USER_PROFILE["check_interval"] // 60
                        msg = (f"⚡️ <b>Crypto Pulse | {exchange_label}</b>\n"
                               f"🔥 <code>{clean_ticker}</code>\n"
                               f"Изменение: <code>{percent_change:.2f}%</code> за {t_min} мин ⏳\n"
                               f"Текущая цена: <code>{current_price} USDT</code>")
                        try:
                            await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML")
                            await asyncio.sleep(0.1)
                        except Exception as e:
                            logging.error(f"Ошибка отправки сообщения: {e}")
                    price_history[symbol] = current_price
            else:
                logging.warning("Нет данных с биржи, спим 20 секунд...")
                await asyncio.sleep(20)
        except Exception as loop_err:
            logging.error(f"Внутренняя ошибка цикла: {loop_err}")
        for _ in range(int(USER_PROFILE["check_interval"])):
            await asyncio.sleep(1)

# ==========================================
# ЧАСТЬ 4: ЧИСТЫЙ АСИНХРОННЫЙ ЗАПУСК
# ==========================================
async def webhook_handle(request):
    return web.Response(text="Crypto Pulse Bot Status: ACTIVE 24/7")

async def main():
    # 1. Запуск веб-сервера aiohttp
    web_app = web.Application()
    web_app.router.add_get('/', webhook_handle)
    
    runner = web.AppRunner(web_app)
    await runner.setup()
    
    port = int(os.getenv("PORT", 7860))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logging.info(f"Веб-сервер успешно запущен на порту {port}")
    
    # 2. Полный сброс зависших сессий вебхуков
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logging.info("Все старые сессии Telegram успешно очищены.")
    except Exception as e:
        logging.error(f"Ошибка очистки сессий: {e}")
    
    # 3. Запуск фонового таска мониторинга
    asyncio.create_task(drops_monitoring_loop())
    
    # 4. Запуск поллинга напрямую в текущем цикле без executor
    try:
        logging.info("Бот запущен и ожидает команд в Telegram...")
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        await storage.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Бот остановлен.")

