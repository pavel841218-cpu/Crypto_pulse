# ==========================================
# ЧАСТЬ 1: БИБЛИОТЕКИ И НАСТРОЙКА ЛОГОВ
# ==========================================
import os
import sys
import json
import asyncio
import logging
import statistics
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

# 💡 Отключаем подробные логи httpx (GET ... 200 OK), чтобы не засорять консоль
logging.getLogger("httpx").setLevel(logging.WARNING)

class SuppressNetworkErrors(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        if "NetworkError" in msg or "ClientOSError" in msg or "Errno 103" in msg:
            return False
        return True

logging.getLogger().addFilter(SuppressNetworkErrors())

# ==========================================
# ЧАСТЬ 2: ТОКЕНЫ И НАСТРОЙКИ (С СОХРАНЕНИЕМ)
# ==========================================
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    logging.critical("BOT_TOKEN не найден! Проверьте переменные окружения.")
    sys.exit(1)

CHAT_ID = int(os.getenv("CHAT_ID", "6908511803"))

bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

SELF_URL = "https://crypto-pulse-43h3.onrender.com"
SETTINGS_FILE = "settings.json"

USER_PROFILE = {
    "exchange": "bingx",
    "alert_percent": 4.0,
    "check_interval": 60,
    "min_price": 0.001,
    "max_price": 1000.0,
    "min_24h_vol": 500000,
    "volume_filter": True,
    "min_rvol": 1.5,
    "volume_inflow_filter": False
}

MANUAL_ALLOWED = set()
MANUAL_BLOCKED = set()
sent_alerts_cooldown = {}

def load_settings():
    """Загрузка настроек из JSON"""
    global USER_PROFILE, MANUAL_ALLOWED, MANUAL_BLOCKED
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                USER_PROFILE.update(data.get("USER_PROFILE", {}))
                MANUAL_ALLOWED = set(data.get("MANUAL_ALLOWED", []))
                MANUAL_BLOCKED = set(data.get("MANUAL_BLOCKED", []))
                logging.info("Настройки успешно загружены из файловой системы.")
        except Exception as e:
            logging.error(f"Ошибка загрузки файлов настроек: {e}")

def save_settings():
    """Сохранение настроек в JSON"""
    try:
        data = {
            "USER_PROFILE": USER_PROFILE,
            "MANUAL_ALLOWED": list(MANUAL_ALLOWED),
            "MANUAL_BLOCKED": list(MANUAL_BLOCKED)
        }
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        logging.error(f"Ошибка сохранения настроек: {e}")

load_settings()

class BotStates(StatesGroup):
    waiting_for_percent = State()
    waiting_for_time = State()
    waiting_for_add_coin = State()
    waiting_for_del_coin = State()

def get_tf_string(seconds):
    minutes = seconds // 60
    if minutes < 5: return "1m"
    elif minutes < 15: return "5m"
    elif minutes < 30: return "15m"
    elif minutes < 120: return "30m"
    elif minutes < 240: return "1h"
    else: return "4h"

def calculate_signal_score(pct, rvol):
    """Расчет силы сигнала от 1 до 5 звезд"""
    score = 1
    abs_pct = abs(pct)
    if abs_pct >= 3.0: score += 1
    if abs_pct >= 7.0: score += 1
    if rvol >= 2.0: score += 1
    if rvol >= 3.5: score += 1
    return min(score, 5)

def detect_volume_inflow(candles, direction="ANY"):
    """
    Анализ притока объема с использованием медианы.
    :param candles: список свечей (dict с ключами 'volume', 'close', 'open')
    :param direction: "LONG", "SHORT" или "ANY"
    :return: (is_inflow: bool, rvol_median: float)
    """
    if len(candles) < 5:
        return False, 1.0
    
    volumes = [float(c['volume']) for c in candles]
    
    curr_vol = volumes[-1]
    curr_close = float(candles[-1]['close'])
    curr_open = float(candles[-1]['open'])
    
    past_vols = volumes[:-1]
    avg_vol = sum(past_vols) / len(past_vols) if past_vols else 1.0
    
    med_vol = statistics.median(past_vols) if past_vols else avg_vol
    base_vol = med_vol if med_vol > 0 else (avg_vol if avg_vol > 0 else 1.0)
    
    rvol_median = curr_vol / base_vol
    
    ramp = (volumes[-1] > volumes[-2] > volumes[-3]) if len(volumes) >= 3 else False
    
    if direction == "LONG":
        direction_ok = curr_close > curr_open
    elif direction == "SHORT":
        direction_ok = curr_close < curr_open
    else:
        direction_ok = curr_close != curr_open
    
    high_vol = curr_vol > (avg_vol * 1.3)
    
    if rvol_median >= 1.5 and direction_ok and (ramp or high_vol):
        return True, round(rvol_median, 1)
    
    return False, round(rvol_median, 1)

def get_main_menu():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    ex_names = {"binance": "Binance 🔸", "bitget": "Bitget 🛡️", "bingx": "BingX 💠"}
    current_ex = ex_names.get(USER_PROFILE["exchange"], USER_PROFILE["exchange"].upper())
    t_min = USER_PROFILE["check_interval"] // 60
    vol_status = "✅ ВКЛ" if USER_PROFILE["volume_filter"] else "❌ ВЫКЛ"
    inflow_status = "✅ ВКЛ" if USER_PROFILE.get("volume_inflow_filter", False) else "❌ ВЫКЛ"
    
    keyboard.inline_keyboard.extend([
        [InlineKeyboardButton(text=f"🏦 Биржа: {current_ex}", callback_data="m_exchange")],
        [InlineKeyboardButton(text=f"📈 Порог: {USER_PROFILE['alert_percent']}%", callback_data="m_percent")],
        [InlineKeyboardButton(text=f"⏳ Интервал: {t_min} мин", callback_data="m_time")],
        [InlineKeyboardButton(text=f"📊 Фильтр Объёма (RVOL): {vol_status}", callback_data="toggle_vol")],
        [InlineKeyboardButton(text=f"🌊 Приток объёма: {inflow_status}", callback_data="toggle_inflow")],
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
    vol_text = "Включен (RVOL > 1.5x)" if USER_PROFILE["volume_filter"] else "Выключен"
    inflow_text = "Включен (рамп + медиана)" if USER_PROFILE.get("volume_inflow_filter", False) else "Выключен"
    return (
        f"⚙️ <b>Панель управления Crypto Pulse 2.2</b>\n\n"
        f"🏦 Биржа: <b>{ex_names.get(USER_PROFILE['exchange'])}</b>\n"
        f"📈 Порог изменения цены: <b>{USER_PROFILE['alert_percent']}%</b>\n"
        f"⏳ Интервал анализа: <b>{t_min} мин.</b>\n"
        f"📊 Фильтр объёма (RVOL): <b>{vol_text}</b>\n"
        f"🌊 Приток объёма: <b>{inflow_text}</b>\n\n"
        f"➕ Белый список: <code>{allowed_str}</code>\n"
        f"❌ Черный список: <code>{blocked_str}</code>\n"
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
    except TelegramBadRequest: pass

@dp.callback_query(F.data == "m_refresh")
async def refresh_panel(callback: types.CallbackQuery):
    try:
        await callback.answer("Обновлено!")
        await callback.message.edit_text(make_profile_text(), parse_mode="HTML", reply_markup=get_main_menu())
    except TelegramBadRequest: pass

@dp.callback_query(F.data == "m_exchange")
async def m_ex_call(callback: types.CallbackQuery):
    try:
        await callback.answer()
        await callback.message.edit_text("🏦 Выбери фьючерсную биржу из списка:", reply_markup=get_exchange_kb())
    except TelegramBadRequest: pass

@dp.callback_query(F.data.startswith("set_ex_"))
async def set_ex(callback: types.CallbackQuery):
    try:
        new_ex = callback.data.split("_")[2]
        USER_PROFILE["exchange"] = new_ex
        save_settings()
        await callback.answer(f"Переключено на {new_ex.upper()}!", show_alert=True)
        await callback.message.edit_text(make_profile_text(), parse_mode="HTML", reply_markup=get_main_menu())
    except TelegramBadRequest: pass

@dp.callback_query(F.data == "toggle_vol")
async def toggle_volume(callback: types.CallbackQuery):
    try:
        USER_PROFILE["volume_filter"] = not USER_PROFILE["volume_filter"]
        save_settings()
        status = "включен" if USER_PROFILE["volume_filter"] else "выключен"
        await callback.answer(f"Фильтр объема {status}!")
        await callback.message.edit_text(make_profile_text(), parse_mode="HTML", reply_markup=get_main_menu())
    except TelegramBadRequest: pass

@dp.callback_query(F.data == "toggle_inflow")
async def toggle_inflow(callback: types.CallbackQuery):
    try:
        USER_PROFILE["volume_inflow_filter"] = not USER_PROFILE.get("volume_inflow_filter", False)
        save_settings()
        status = "включен" if USER_PROFILE["volume_inflow_filter"] else "выключен"
        await callback.answer(f"Фильтр притока объема {status}!")
        await callback.message.edit_text(make_profile_text(), parse_mode="HTML", reply_markup=get_main_menu())
    except TelegramBadRequest: pass

@dp.callback_query(F.data == "m_percent")
async def m_pct_call(callback: types.CallbackQuery):
    try:
        await callback.answer()
        await callback.message.edit_text("📈 Выбери порог изменения цены:", reply_markup=get_percent_kb())
    except TelegramBadRequest: pass

@dp.callback_query(F.data.startswith("set_pct_"))
async def set_pct(callback: types.CallbackQuery):
    try:
        new_pct = float(callback.data.split("_")[2])
        USER_PROFILE["alert_percent"] = new_pct
        save_settings()
        await callback.answer("Процент обновлен!")
        await callback.message.edit_text(make_profile_text(), parse_mode="HTML", reply_markup=get_main_menu())
    except TelegramBadRequest: pass

@dp.callback_query(F.data == "inp_pct")
async def inp_pct(callback: types.CallbackQuery, state: FSMContext):
    try:
        await callback.answer()
        await state.set_state(BotStates.waiting_for_percent)
        await callback.message.answer("✏️ Введи процент изменения цены (от 1 до 100):")
    except TelegramBadRequest: pass

@dp.message(BotStates.waiting_for_percent)
async def proc_custom_pct(message: types.Message, state: FSMContext):
    try:
        val = round(float(message.text.strip().replace(",", ".")), 2)
        if 1.0 <= val <= 100.0:
            USER_PROFILE["alert_percent"] = val
            save_settings()
            await state.clear()
            await message.answer(f"✅ Установлен порог в {val}%!", reply_markup=get_main_menu())
        else: await message.answer("❌ Введи число от 1 до 100:")
    except ValueError: await message.answer("❌ Отправь корректное число цифрами:")

@dp.callback_query(F.data == "m_time")
async def m_time_call(callback: types.CallbackQuery):
    try:
        await callback.answer()
        await callback.message.edit_text("⏳ Выбери интервал сканирования рынка:", reply_markup=get_time_kb())
    except TelegramBadRequest: pass

@dp.callback_query(F.data.startswith("set_t_"))
async def set_time(callback: types.CallbackQuery):
    try:
        minutes = int(callback.data.split("_")[2])
        USER_PROFILE["check_interval"] = minutes * 60
        save_settings()
        await callback.answer(f"Таймфрейм изменен на {minutes} мин.!")
        await callback.message.edit_text(make_profile_text(), parse_mode="HTML", reply_markup=get_main_menu())
    except TelegramBadRequest: pass

@dp.callback_query(F.data == "inp_t")
async def inp_time(callback: types.CallbackQuery, state: FSMContext):
    try:
        await callback.answer()
        await state.set_state(BotStates.waiting_for_time)
        await callback.message.answer("✏️ Введи интервал сканирования в минутах:")
    except TelegramBadRequest: pass

@dp.message(BotStates.waiting_for_time)
async def proc_custom_time(message: types.Message, state: FSMContext):
    if message.text.isdigit():
        minutes = int(message.text)
        if minutes >= 1:
            USER_PROFILE["check_interval"] = minutes * 60
            save_settings()
            await state.clear()
            await message.answer(f"✅ Интервал обновлен: {minutes} мин.!", reply_markup=get_main_menu())
            return
    await message.answer("❌ Введи корректное число:")

@dp.callback_query(F.data == "coin_add")
async def coin_add_call(callback: types.CallbackQuery, state: FSMContext):
    try:
        await callback.answer()
        await state.set_state(BotStates.waiting_for_add_coin)
        await callback.message.answer("➕ Напиши тикер монеты (например: BTC или SOL):")
    except TelegramBadRequest: pass

@dp.message(BotStates.waiting_for_add_coin)
async def proc_coin_add(message: types.Message, state: FSMContext):
    coin = message.text.strip().upper().replace("USDT", "")
    if coin:
        if coin in MANUAL_BLOCKED: MANUAL_BLOCKED.remove(coin)
        MANUAL_ALLOWED.add(coin)
        save_settings()
        await state.clear()
        await message.answer(f"✅ Монета {coin} добавлена!", reply_markup=get_main_menu())

@dp.callback_query(F.data == "coin_del")
async def coin_del_call(callback: types.CallbackQuery, state: FSMContext):
    try:
        await callback.answer()
        await state.set_state(BotStates.waiting_for_del_coin)
        await callback.message.answer("❌ Напиши тикер для внесения в черный список:")
    except TelegramBadRequest: pass

@dp.message(BotStates.waiting_for_del_coin)
async def proc_coin_del(message: types.Message, state: FSMContext):
    coin = message.text.strip().upper().replace("USDT", "")
    if coin:
        if coin in MANUAL_ALLOWED: MANUAL_ALLOWED.remove(coin)
        MANUAL_BLOCKED.add(coin)
        save_settings()
        await state.clear()
        await message.answer(f"❌ Монета {coin} заблокирована!", reply_markup=get_main_menu())

# ==========================================
# ЧАСТЬ 4: ПОЛНЫЙ АНАЛИЗ РЫНКА
# ==========================================
async def fetch_active_symbols(client, exchange):
    """Получает ВСЕ активные пары с учетом фильтра 24-часового объема"""
    symbols = []
    try:
        if exchange == "bingx":
            url = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"
            res = await client.get(url, timeout=6.0)
            if res.status_code == 200:
                for item in res.json().get("data", []):
                    sym = item.get('symbol', '')
                    vol = float(item.get('quoteVolume', 0))
                    if sym.endswith("-USDT") and vol >= USER_PROFILE["min_24h_vol"]:
                        symbols.append(sym)
        elif exchange == "binance":
            url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
            res = await client.get(url, timeout=6.0)
            if res.status_code == 200:
                for item in res.json():
                    sym = item.get('symbol', '')
                    vol = float(item.get('quoteVolume', 0))
                    if sym.endswith("USDT") and vol >= USER_PROFILE["min_24h_vol"]:
                        symbols.append(sym)
    except Exception as e:
        logging.error(f"Ошибка получения тикеров: {e}")
    return symbols

async def analyze_symbol_klines(client, symbol, exchange, interval_str):
    """Анализ свечей с сортировкой и детектором притока объёма"""
    try:
        if exchange == "bingx":
            clean_sym = symbol.replace("-", "")
            url = "https://open-api.bingx.com/openApi/swap/v3/quote/klines"
            params = {"symbol": clean_sym, "interval": interval_str, "limit": 21}
            res = await client.get(url, params=params, timeout=4.0)
            if res.status_code == 200:
                raw_candles = res.json().get("data", [])
                if len(raw_candles) >= 5:
                    # 💡 Сортировка от старых свечей к новым
                    candles = sorted(raw_candles, key=lambda x: int(x['time']))
                    
                    last_candle = candles[-1]
                    c_open = float(last_candle['open'])
                    c_close = float(last_candle['close'])
                    c_vol = float(last_candle['volume'])
                    
                    past_candles = candles[:-1]
                    avg_vol = sum([float(c['volume']) for c in past_candles]) / len(past_candles)
                    rvol = c_vol / avg_vol if avg_vol > 0 else 1.0
                    
                    pct_change = ((c_close - c_open) / c_open) * 100
                    
                    inflow_detected = False
                    inflow_rvol = 1.0
                    if USER_PROFILE.get("volume_inflow_filter", False):
                        candle_dicts = [
                            {
                                'volume': c['volume'],
                                'close': c['close'],
                                'open': c['open']
                            }
                            for c in candles
                        ]
                        inflow_detected, inflow_rvol = detect_volume_inflow(candle_dicts, direction="ANY")
                    
                    return c_close, round(pct_change, 2), round(rvol, 2), inflow_detected, round(inflow_rvol, 2)

        elif exchange == "binance":
            url = "https://fapi.binance.com/fapi/v1/klines"
            params = {"symbol": symbol, "interval": interval_str, "limit": 21}
            res = await client.get(url, params=params, timeout=4.0)
            if res.status_code == 200:
                candles = res.json()
                if len(candles) >= 5:
                    last_candle = candles[-1]
                    c_open = float(last_candle[1])
                    c_close = float(last_candle[4])
                    c_vol = float(last_candle[5])
                    
                    past_candles = candles[:-1]
                    avg_vol = sum([float(c[5]) for c in past_candles]) / len(past_candles)
                    rvol = c_vol / avg_vol if avg_vol > 0 else 1.0
                    
                    pct_change = ((c_close - c_open) / c_open) * 100
                    
                    inflow_detected = False
                    inflow_rvol = 1.0
                    if USER_PROFILE.get("volume_inflow_filter", False):
                        candle_dicts = [
                            {
                                'volume': c[5],
                                'close': c[4],
                                'open': c[1]
                            }
                            for c in candles
                        ]
                        inflow_detected, inflow_rvol = detect_volume_inflow(candle_dicts, direction="ANY")
                    
                    return c_close, round(pct_change, 2), round(rvol, 2), inflow_detected, round(inflow_rvol, 2)
    except Exception:
        pass
    return None, None, None, False, 1.0

async def keep_alive_ping():
    await asyncio.sleep(30)
    logging.info("Анти-сон система активирована.")
    while True:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.get(SELF_URL)
        except Exception: pass
        await asyncio.sleep(240)

async def drops_monitoring_loop():
    logging.info("Фоновый сканер Crypto Pulse v2.2 запущен.")
    
    async with httpx.AsyncClient(timeout=8.0) as client:
        while True:
            try:
                exchange = USER_PROFILE["exchange"]
                threshold = USER_PROFILE["alert_percent"]
                t_seconds = USER_PROFILE["check_interval"]
                t_min = t_seconds // 60
                tf_str = get_tf_string(t_seconds)
                
                symbols = await fetch_active_symbols(client, exchange)
                semaphore = asyncio.Semaphore(10) # 💡 Ограничение конкурентных запросов
                
                async def check_single(sym):
                    clean_ticker = sym.replace('-', '').replace('USDT', '')
                    if clean_ticker in MANUAL_BLOCKED: return
                    
                    async with semaphore:
                        await asyncio.sleep(0.05) # 💡 Пауза 50мс для защиты от бана IP
                        price, pct, rvol, inflow_detected, inflow_rvol = await analyze_symbol_klines(
                            client, sym, exchange, tf_str
                        )
                    
                    if price is None or pct is None: return
                    
                    if not (USER_PROFILE["min_price"] <= price <= USER_PROFILE["max_price"]):
                        if clean_ticker not in MANUAL_ALLOWED: return
                    
                    # Фильтр притока объема
                    if USER_PROFILE.get("volume_inflow_filter", False):
                        if not inflow_detected:
                            return
                    
                    if abs(pct) >= threshold:
                        if USER_PROFILE["volume_filter"] and rvol < USER_PROFILE["min_rvol"]:
                            return
                        
                        now_time = asyncio.get_event_loop().time()
                        if sym in sent_alerts_cooldown and (now_time - sent_alerts_cooldown[sym]) < 900:
                            return
                        
                        sent_alerts_cooldown[sym] = now_time
                        
                        score = calculate_signal_score(pct, rvol)
                        stars = "⭐" * score
                        
                        if inflow_detected:
                            vol_icon = "🌊 Приток объёма"
                        elif rvol >= 2.0:
                            vol_icon = "🔥 High Vol"
                        else:
                            vol_icon = "📊 Normal Vol"
                        
                        icon = "🚀 LONG" if pct > 0 else "🔻 SHORT"
                        
                        msg = (
                            f"{icon} | <b>Crypto Pulse ({exchange.upper()})</b>\n"
                            f"🔥 <code>{clean_ticker}USDT</code>\n\n"
                            f"Сила сигнала: <b>{stars} ({score}/5)</b>\n"
                            f"Изменение: <b>{pct}%</b> за <b>{t_min} мин</b> ⏳\n"
                            f"Объём (RVOL): <b>x{rvol}</b> ({vol_icon})\n"
                            f"Цена: <code>{price} USDT</code>"
                        )
                        
                        if inflow_detected:
                            msg += f"\n🌊 Приток (медиана): <b>x{inflow_rvol}</b>"
                        
                        try:
                            await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML")
                        except Exception as e:
                            logging.error(f"Ошибка отправки Telegram: {e}")

                if symbols:
                    tasks = [check_single(sym) for sym in symbols]
                    await asyncio.gather(*tasks)

            except Exception as loop_err:
                logging.error(f"Ошибка цикла: {loop_err}")
            
            await asyncio.sleep(30)

# ==========================================
# ЧАСТЬ 5: ЗАПУСК ВЕБ-СЕРВЕРА И БОТА
# ==========================================
async def webhook_handle(request):
    return web.Response(text="Crypto Pulse 2.2 Active")

async def main():
    web_app = web.Application()
    web_app.router.add_get('/', webhook_handle)
    
    runner = web.AppRunner(web_app)
    await runner.setup()
    
    port = int(os.getenv("PORT", 7860))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception: pass
    
    asyncio.create_task(drops_monitoring_loop())
    asyncio.create_task(keep_alive_ping())
    
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        await storage.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Бот остановлен.")
