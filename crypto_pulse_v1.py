import asyncio
import hmac
import hashlib
import json
import logging
import time
from collections import deque
import aiohttp
from curl_cffi import requests

# ==========================================
# 1. КОНФИГУРАЦИЯ
# ==========================================
SYMBOL = "SAGAUSDT"

# Telegram Настройки
TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"

# BingX API Настройки
BINGX_API_KEY = "YOUR_BINGX_API_KEY"
BINGX_SECRET_KEY = "YOUR_BINGX_SECRET_KEY"
BINGX_URL = "https://open-api.bingx.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ==========================================
# 2. УТИЛИТЫ И ПОДПИСЬ BINGX API
# ==========================================
def generate_bingx_signature(secret_key: str, payload: str) -> str:
    return hmac.new(secret_key.encode('utf-8'), payload.encode('utf-8'), hashlib.sha256).hexdigest()

async def send_telegram(text: str):
    if TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        logging.info(f"[TG MOCK]:\n{text}")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(url, json=payload)
    except Exception as e:
        logging.error(f"Ошибка отправки Telegram: {e}")

def calculate_ema(prices: list, period: int = 7) -> float:
    if len(prices) < period:
        return prices[-1] if prices else 0.0
    k = 2 / (period + 1)
    ema = prices[0]
    for price in prices[1:]:
        ema = (price * k) + (ema * (1 - k))
    return ema

# ==========================================
# 3. BINANCE DATA ENGINE (curl_cffi + Bypass 418)
# ==========================================
class BinanceRVolEngine:
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.volumes_5m = deque(maxlen=30)
        # Основной и резервные эндпоинты Binance для обхода блокировок IP Render
        self.endpoints = [
            "https://fapi.binance.com/fapi/v1/klines",
            "https://fapi1.binance.com/fapi/v1/klines",
            "https://fapi2.binance.com/fapi/v1/klines",
            "https://fapi3.binance.com/fapi/v1/klines"
        ]

    def load_initial_history(self) -> bool:
        """Загрузка 5M свечей с Binance с полным обходом WAF/HTTP 418."""
        params = {"symbol": self.symbol, "interval": "5m", "limit": 30}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive"
        }

        for endpoint in self.endpoints:
            for attempt in range(1, 3):
                try:
                    session = requests.Session(impersonate="chrome120")
                    res = session.get(endpoint, params=params, headers=headers, timeout=10)
                    
                    if res.status_code == 200:
                        data = res.json()
                        self.volumes_5m.clear()
                        for candle in data:
                            self.volumes_5m.append(float(candle[7]))  # Quote USDT Volume
                        logging.info(f"[{self.symbol}] Успешно загружено {len(self.volumes_5m)} свечей через {endpoint}")
                        return True
                    
                    logging.warning(f"[{self.symbol}] {endpoint} дал статус {res.status_code}. Попытка {attempt}...")
                    time.sleep(1)
                except Exception as e:
                    logging.error(f"[{self.symbol}] Ошибка подключения к {endpoint}: {e}")
                    time.sleep(1)

        logging.error(f"[{self.symbol}] Все эндпоинты Binance заблокировали запрос.")
        return False

    def get_rvol_metrics(self) -> dict:
        if len(self.volumes_5m) < 21:
            return {"status": "WAIT_DATA"}

        v_list = list(self.volumes_5m)
        history_20 = v_list[-21:-1]
        sma_20 = sum(history_20) / len(history_20)

        if sma_20 == 0:
            return {"status": "ZERO_SMA"}

        rvol_curr = round(v_list[-1] / sma_20, 2)
        rvol_prev = round(v_list[-2] / sma_20, 2)

        # Детекция ловушки кульминации (Spike из флета)
        is_buying_climax = (rvol_curr > 4.0) and (rvol_prev < 1.8)
        # Органическое нарастание
        is_healthy_trend = (2.0 <= rvol_curr <= 4.0) or (rvol_curr > 4.0 and rvol_prev >= 1.8)

        return {
            "status": "OK",
            "rvol": rvol_curr,
            "rvol_prev": rvol_prev,
            "is_buying_climax": is_buying_climax,
            "is_healthy_trend": is_healthy_trend
        }

# ==========================================
# 4. BINGX EXECUTION ENGINE (REST API)
# ==========================================
class BingXExecutionEngine:
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()

    def validate_1m_pinbar(self, kline: dict) -> bool:
        """Проверка геометрии 1M-свечи на фитиль от 50%."""
        o, h, l, c = kline['open'], kline['high'], kline['low'], kline['close']
        rng = h - l
        if rng == 0:
            return False
        upper_wick = h - max(o, c)
        return (upper_wick / rng >= 0.50) and (c < (l + rng * 0.45))

    async def place_order(self, side: str, position_side: str, order_type: str, quantity: float, stop_price: float = None):
        if BINGX_API_KEY == "YOUR_BINGX_API_KEY":
            logging.info(f"[BINGX MOCK ORDER]: {side} {position_side} | Type: {order_type} | Qty: {quantity}")
            return True

        endpoint = "/openApi/swap/v2/trade/order"
        timestamp = int(time.time() * 1000)
        
        params = {
            "symbol": f"{self.symbol}-USDT",
            "side": side,
            "positionSide": position_side,
            "type": order_type,
            "quantity": quantity,
            "timestamp": timestamp
        }
        if stop_price:
            params["stopPrice"] = stop_price

        param_str = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
        signature = generate_bingx_signature(BINGX_SECRET_KEY, param_str)
        url = f"{BINGX_URL}{endpoint}?{param_str}&signature={signature}"

        headers = {"X-BX-APIKEY": BINGX_API_KEY}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers) as res:
                    data = await res.json()
                    logging.info(f"BingX Order Response: {data}")
                    return data.get("code") == 0
        except Exception as e:
            logging.error(f"Ошибка вызова BingX API: {e}")
            return False

    async def execute_market_short(self, entry_price: float, stop_loss: float, qty: float = 10.0):
        success = await self.place_order(side="SELL", position_side="SHORT", order_type="MARKET", quantity=qty)
        if success:
            await self.place_order(side="BUY", position_side="SHORT", order_type="STOP_MARKET", quantity=qty, stop_price=stop_loss)
            await send_telegram(
                f"🔴 *[BINGX SHORT EXECUTED]*\n"
                f"Монета: `#{self.symbol}`\n"
                f"Вход: `{entry_price}`\n"
                f"Stop-Loss: `{stop_loss}`\n"
                f"Причина: 1M Pinbar + Пробой EMA7 (DANGER)"
            )

    async def execute_market_long(self, entry_price: float, qty: float = 10.0):
        success = await self.place_order(side="BUY", position_side="LONG", order_type="MARKET", quantity=qty)
        if success:
            await send_telegram(
                f"🚀 *[BINGX LONG EXECUTED]*\n"
                f"Монета: `#{self.symbol}`\n"
                f"Вход: `{entry_price}`\n"
                f"Причина: Органический тренд на микрооткате"
            )

# ==========================================
# 5. ОСНОВНОЙ АСИНХРОННЫЙ ЦИКЛ (QUASIMODO CORE)
# ==========================================
class QuasimodoBotCore:
    def __init__(self, symbol: str):
        self.symbol = symbol.lower()
        self.binance_engine = BinanceRVolEngine(symbol)
        self.bingx_executor = BingXExecutionEngine(symbol)
        
        self.is_short_active = False
        self.short_timer = 0
        self.bingx_oi_delta = 0.0
        self.closes_1m = deque(maxlen=50)

    async def start(self):
        if not self.binance_engine.load_initial_history():
            logging.error("Сбой инициализации буфера. Завершение работы.")
            return

        await self.subscribe_binance_ws()

    async def subscribe_binance_ws(self):
        url = f"wss://fstream.binance.com/ws/{self.symbol}@kline_1m"
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url) as ws:
                logging.info(f"[{self.symbol.upper()}] Подключен асинхронный WS 1M Binance...")
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        k = data['k']
                        close_p = float(k['c'])
                        is_closed = k['x']

                        self.closes_1m.append(close_p)
                        ema7 = calculate_ema(list(self.closes_1m), period=7)

                        # Обновление незакрытого объема 5M-свечи на лету
                        quote_vol = float(k['q'])
                        if self.binance_engine.volumes_5m:
                            self.binance_engine.volumes_5m[-1] = quote_vol

                        if is_closed:
                            kline_dict = {
                                'open': float(k['o']),
                                'high': float(k['h']),
                                'low': float(k['l']),
                                'close': close_p
                            }
                            await self.on_1m_candle_close(kline_dict, ema7)

    async def on_1m_candle_close(self, kline: dict, ema7: float):
        metrics = self.binance_engine.get_rvol_metrics()
        if metrics.get("status") != "OK":
            return

        rvol = metrics["rvol"]
        rvol_prev = metrics["rvol_prev"]

        # --- ДЕТЕКЦИЯ СЕПАРАТОРА DANGER ---
        if metrics["is_buying_climax"] or self.bingx_oi_delta < -1.5:
            if not self.is_short_active:
                self.is_short_active = True
                self.short_timer = 15
                await send_telegram(
                    f"⚠️ *[DANGER - SHORT REVERSAL]*\n"
                    f"Монета: `#{self.symbol.upper()}`\n"
                    f"RVOL 5M: `{rvol}x` (Пред: `{rvol_prev}x`)\n"
                    f"BingX OI Delta: `{self.bingx_oi_delta}%`\n"
                    f"Статус: Лонг заблокирован. Поиск 1M-фитиля для *ШОРТА* (15м)!"
                )

        # --- ОБРАБОТКА ШОРТ-ВХОДА ПО 1M ФИТИЛЮ ---
        if self.is_short_active:
            is_pinbar = self.bingx_executor.validate_1m_pinbar(kline)
            if is_pinbar and (kline['close'] < ema7):
                stop_loss = round(kline['high'] * 1.001, 5)
                await self.bingx_executor.execute_market_short(kline['close'], stop_loss)
                self.is_short_active = False
                return

            self.short_timer -= 1
            if self.short_timer <= 0:
                self.is_short_active = False
                logging.info(f"[{self.symbol.upper()}] Истекло время ожидания фитиля DANGER.")

        # --- ОБРАБОТКА ОРГАНИЧЕСКОГО ЛОНГА ---
        if metrics["is_healthy_trend"] and self.bingx_oi_delta > 0.5 and not self.is_short_active:
            if kline['close'] <= ema7:
                await self.bingx_executor.execute_market_long(kline['close'])

# ==========================================
# 6. ТОЧКА ВХОДА
# ==========================================
if __name__ == "__main__":
    bot = QuasimodoBotCore(symbol=SYMBOL)
    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        logging.info("Бот остановлен пользователем.")
