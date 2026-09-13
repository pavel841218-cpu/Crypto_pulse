import asyncio
import hmac
import hashlib
import json
import logging
import os
import time
import traceback
from collections import deque
import aiohttp
from aiohttp import web
from curl_cffi.requests import AsyncSession

# ==========================================
# 1. КОНФИГУРАЦИЯ
# ==========================================
SYMBOL = "SAGAUSDT"
SYMBOL_BINGX = "SAGA-USDT"  # Формат BingX

TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"

BINGX_API_KEY = "YOUR_BINGX_API_KEY"
BINGX_SECRET_KEY = "YOUR_BINGX_SECRET_KEY"
BINGX_URL = "https://open-api.bingx.com"

BYBIT_URL = "https://api.bybit.com"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ==========================================
# 2. УТИЛИТЫ
# ==========================================

def generate_bingx_signature(secret_key: str, payload: str) -> str:
    return hmac.new(
        secret_key.encode('utf-8'),
        payload.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()


async def send_telegram(text: str):
    if TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        logging.info(f"[TG MOCK]:\n{text}")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as resp:
                if resp.status != 200:
                    logging.warning(f"Telegram вернул статус {resp.status}")
    except Exception as e:
        logging.error(f"Ошибка отправки Telegram: {e}")


def calculate_ema(prices: list, period: int = 7) -> float:
    if not prices:
        return 0.0
    if len(prices) < period:
        return prices[-1]
    k = 2 / (period + 1)
    ema = prices[0]
    for price in prices[1:]:
        ema = (price * k) + (ema * (1 - k))
    return ema


# ==========================================
# 3. BINANCE DATA ENGINE (с Bybit-фолбэком)
# ==========================================

class BinanceRVolEngine:
    """
    Собирает объёмы 5m свечей.
    Пробует Binance через curl_cffi (impersonate Chrome).
    Если Binance блокирует — использует Bybit.
    """

    def __init__(self, symbol: str):
        # symbol в формате "SAGAUSDT"
        self.symbol = symbol.upper()
        self.volumes_5m = deque(maxlen=30)
        self.data_source = "none"

    async def load_initial_history_async(self) -> bool:
        """Загружает 30 свечей 5m для RVOL."""

        # ------------------------------
        # ЭТАП 1: BINANCE через curl_cffi (async)
        # ------------------------------
        endpoints = [
            "https://fapi.binance.com/fapi/v1/klines",
            "https://fapi1.binance.com/fapi/v1/klines",
            "https://fapi2.binance.com/fapi/v1/klines",
        ]
        params = {"symbol": self.symbol, "interval": "5m", "limit": 30}

        for endpoint in endpoints:
            try:
                logging.info(f"[{self.symbol}] Запрос Binance: {endpoint}")
                async with AsyncSession() as s:
                    res = await s.get(
                        endpoint,
                        params=params,
                        impersonate="chrome120",
                        timeout=8
                    )
                    logging.info(
                        f"[{self.symbol}] Binance {endpoint} → HTTP {res.status_code}"
                    )
                    if res.status_code == 200:
                        data = res.json()
                        if isinstance(data, list) and len(data) >= 20:
                            self.volumes_5m.clear()
                            for candle in data:
                                self.volumes_5m.append(float(candle[7]))
                            self.data_source = "Binance"
                            logging.info(
                                f"[{self.symbol}] ✅ История Binance загружена "
                                f"({len(data)} свечей)"
                            )
                            return True
                    elif res.status_code in (418, 429, 451, 403):
                        logging.warning(
                            f"[{self.symbol}] ⚠️ Binance блокирует "
                            f"(HTTP {res.status_code})"
                        )
                        break
            except Exception as e:
                logging.warning(f"[{self.symbol}] Binance {endpoint}: {e}")

        # ------------------------------
        # ЭТАП 2: BYBIT FALLBACK
        # ------------------------------
        logging.warning(
            f"[{self.symbol}] Binance недоступен → переключаюсь на Bybit"
        )
        return await self._load_from_bybit()

    async def _load_from_bybit(self) -> bool:
        """Bybit V5 kline endpoint."""
        url = f"{BYBIT_URL}/v5/market/kline"
        params = {
            "category": "linear",
            "symbol": self.symbol,
            "interval": "5",
            "limit": 30,
        }
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, params=params, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    logging.info(f"[{self.symbol}] Bybit → HTTP {resp.status}")
                    if resp.status != 200:
                        logging.error(f"[{self.symbol}] Bybit вернул {resp.status}")
                        return False

                    data = await resp.json()
                    if data.get("retCode") != 0:
                        logging.error(f"[{self.symbol}] Bybit retCode: {data.get('retMsg')}")
                        return False

                    lst = data.get("result", {}).get("list", [])
                    if not lst or len(lst) < 20:
                        logging.error(f"[{self.symbol}] Bybit: мало свечей ({len(lst)})")
                        return False

                    # Bybit отдаёт от новых к старым — разворачиваем
                    lst.reverse()
                    self.volumes_5m.clear()
                    for candle in lst:
                        # candle = [start, open, high, low, close, volume, turnover]
                        turnover = float(candle[6])  # в USDT
                        self.volumes_5m.append(turnover)

                    self.data_source = "Bybit"
                    logging.info(
                        f"[{self.symbol}] ✅ История Bybit загружена "
                        f"({len(lst)} свечей)"
                    )
                    return True

        except Exception as e:
            logging.error(f"[{self.symbol}] Bybit ошибка: {e}")
            return False

    def get_rvol_metrics(self) -> dict:
        if len(self.volumes_5m) < 21:
            return {"status": "WAIT_DATA", "have": len(self.volumes_5m)}

        v_list = list(self.volumes_5m)
        history_20 = v_list[-21:-1]
        sma_20 = sum(history_20) / len(history_20)

        if sma_20 == 0:
            return {"status": "ZERO_SMA"}

        rvol_curr = round(v_list[-1] / sma_20, 2)
        rvol_prev = round(v_list[-2] / sma_20, 2)

        is_buying_climax = (rvol_curr > 4.0) and (rvol_prev < 1.8)
        is_healthy_trend = (
            (2.0 <= rvol_curr <= 4.0)
            or (rvol_curr > 4.0 and rvol_prev >= 1.8)
        )

        return {
            "status": "OK",
            "rvol": rvol_curr,
            "rvol_prev": rvol_prev,
            "is_buying_climax": is_buying_climax,
            "is_healthy_trend": is_healthy_trend,
            "source": self.data_source,
        }


# ==========================================
# 4. BINGX OI FETCHER
# ==========================================

async def fetch_bingx_oi_delta(symbol_bingx: str) -> float:
    """
    Возвращает процент изменения OI BingX за последние 5 минут.
    """
    url = f"{BINGX_URL}/openApi/swap/v2/quote/openInterestHistory"
    params = {"symbol": symbol_bingx, "interval": "5m", "limit": 3}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, params=params,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status != 200:
                    logging.debug(f"[OI] BingX HTTP {resp.status}")
                    return 0.0

                data = await resp.json()
                lst = data.get("data", [])
                if not isinstance(lst, list) or len(lst) < 2:
                    logging.debug(f"[OI] BingX: мало данных ({len(lst) if lst else 0})")
                    return 0.0

                try:
                    prev = float(lst[-2].get("openInterest", 0))
                    curr = float(lst[-1].get("openInterest", 0))
                except (ValueError, TypeError):
                    return 0.0

                if prev <= 0:
                    return 0.0

                delta = ((curr - prev) / prev) * 100
                logging.debug(f"[OI] BingX {symbol_bingx}: {delta:+.2f}%")
                return delta
    except Exception as e:
        logging.debug(f"[OI] BingX ошибка: {e}")
        return 0.0


# ==========================================
# 5. BINGX EXECUTION ENGINE
# ==========================================

class BingXExecutionEngine:
    def __init__(self, symbol_bingx: str):
        self.symbol = symbol_bingx  # "SAGA-USDT"

    def validate_1m_pinbar(self, kline: dict) -> bool:
        o, h, l, c = kline['open'], kline['high'], kline['low'], kline['close']
        rng = h - l
        if rng == 0:
            return False
        upper_wick = h - max(o, c)
        return (upper_wick / rng >= 0.50) and (c < (l + rng * 0.45))

    async def place_order(
        self, side: str, position_side: str,
        order_type: str, quantity: float, stop_price: float = None
    ):
        if BINGX_API_KEY == "YOUR_BINGX_API_KEY":
            logging.info(
                f"[BINGX MOCK] {side} {position_side} | "
                f"{order_type} | qty={quantity} | stop={stop_price}"
            )
            return True

        endpoint = "/openApi/swap/v2/trade/order"
        timestamp = int(time.time() * 1000)

        params = {
            "symbol": self.symbol,
            "side": side,
            "positionSide": position_side,
            "type": order_type,
            "quantity": quantity,
            "timestamp": timestamp,
        }
        if stop_price:
            params["stopPrice"] = stop_price

        param_str = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
        signature = generate_bingx_signature(BINGX_SECRET_KEY, param_str)
        url = f"{BINGX_URL}{endpoint}?{param_str}&signature={signature}"
        headers = {"X-BX-APIKEY": BINGX_API_KEY}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, timeout=10) as res:
                    data = await res.json()
                    logging.info(f"[BINGX ORDER] {data}")
                    return data.get("code") == 0
        except Exception as e:
            logging.error(f"[BINGX ORDER ERROR] {e}")
            return False

    async def execute_market_short(self, entry_price, stop_loss, qty=10.0):
        logging.info(f"[SHORT] {self.symbol} @ {entry_price}, SL={stop_loss}")
        ok = await self.place_order(
            side="SELL", position_side="SHORT",
            order_type="MARKET", quantity=qty
        )
        if ok:
            await self.place_order(
                side="BUY", position_side="SHORT",
                order_type="STOP_MARKET", quantity=qty, stop_price=stop_loss
            )
            await send_telegram(
                f"🔴 *[BINGX SHORT EXECUTED]*\n"
                f"Монета: `#{self.symbol}`\n"
                f"Вход: `{entry_price}`\n"
                f"Stop: `{stop_loss}`\n"
                f"Причина: 1M Pinbar + пробой EMA7"
            )

    async def execute_market_long(self, entry_price, qty=10.0):
        logging.info(f"[LONG] {self.symbol} @ {entry_price}")
        ok = await self.place_order(
            side="BUY", position_side="LONG",
            order_type="MARKET", quantity=qty
        )
        if ok:
            await send_telegram(
                f"🚀 *[BINGX LONG EXECUTED]*\n"
                f"Монета: `#{self.symbol}`\n"
                f"Вход: `{entry_price}`"
            )


# ==========================================
# 6. QUASIMODO CORE
# ==========================================

class QuasimodoBotCore:
    def __init__(self, symbol: str, symbol_bingx: str):
        self.symbol = symbol.lower()          # "sagausdt"
        self.symbol_bingx = symbol_bingx      # "SAGA-USDT"
        self.binance_engine = BinanceRVolEngine(symbol)
        self.bingx_executor = BingXExecutionEngine(symbol_bingx)

        self.is_short_active = False
        self.short_timer = 0
        self.bingx_oi_delta = 0.0
        self.closes_1m = deque(maxlen=50)

        # Счётчики для логов
        self.ws_msg_count = 0
        self.closed_candles = 0

    async def start(self):
        if not await self.binance_engine.load_initial_history_async():
            logging.error(f"[{self.symbol}] Сбой инициализации буфера. Завершение.")
            return

        logging.info(
            f"[{self.symbol}] Источник данных: {self.binance_engine.data_source}"
        )

        # Запускаем WS с авто-переподключением
        while True:
            try:
                await self.subscribe_binance_ws()
            except Exception as e:
                logging.error(f"[{self.symbol}] WS упал: {e}")
                logging.error(traceback.format_exc())
                logging.info(f"[{self.symbol}] Переподключение через 5с...")
                await asyncio.sleep(5)

    async def subscribe_binance_ws(self):
        url = f"wss://fstream.binance.com/ws/{self.symbol}@kline_1m"
        logging.info(f"[{self.symbol}] Подключаю WS: {url}")

        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url, heartbeat=30) as ws:
                logging.info(f"[{self.symbol}] ✅ WS 1m подключен")

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        self.ws_msg_count += 1

                        if self.ws_msg_count % 60 == 0:
                            logging.info(
                                f"[{self.symbol}] WS: получено "
                                f"{self.ws_msg_count} сообщений"
                            )

                        try:
                            data = json.loads(msg.data)
                            k = data.get('k', {})
                            if not k:
                                continue

                            close_p = float(k['c'])
                            is_closed = k['x']

                            self.closes_1m.append(close_p)
                            ema7 = calculate_ema(list(self.closes_1m), period=7)

                            # ⚠️ ВАЖНО: не перезаписываем 5m буфер
                            # RVOL работает по ЗАКРЫТЫМ 5m свечам.
                            # Обновляем только последний слот, но НЕ искажаем
                            # (можно собирать 1m объёмы и аккумулировать их в 5m)
                            # Простое решение: раз в 5 минут обновлять буфер с REST.

                            if is_closed:
                                self.closed_candles += 1
                                kline_dict = {
                                    'open': float(k['o']),
                                    'high': float(k['h']),
                                    'low': float(k['l']),
                                    'close': close_p,
                                }
                                await self.on_1m_candle_close(kline_dict, ema7)

                        except Exception as e:
                            logging.error(f"[{self.symbol}] Ошибка обработки WS: {e}")

                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logging.error(f"[{self.symbol}] WS ошибка: {ws.exception()}")
                        break
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                        logging.warning(f"[{self.symbol}] WS закрыт")
                        break

    async def on_1m_candle_close(self, kline: dict, ema7: float):
        # Обновляем OI BingX (раз в 1m свечу)
        try:
            self.bingx_oi_delta = await fetch_bingx_oi_delta(self.symbol_bingx)
        except Exception as e:
            logging.debug(f"[{self.symbol}] OI fetch error: {e}")

        metrics = self.binance_engine.get_rvol_metrics()
        if metrics.get("status") != "OK":
            logging.debug(
                f"[{self.symbol}] RVOL не готов: {metrics.get('status')} "
                f"({metrics.get('have', '?')} свечей)"
            )
            return

        rvol = metrics["rvol"]
        rvol_prev = metrics["rvol_prev"]

        logging.info(
            f"[{self.symbol}] 1m close={kline['close']} | "
            f"EMA7={ema7:.5f} | RVOL={rvol}x (prev {rvol_prev}x) | "
            f"OI Δ={self.bingx_oi_delta:+.2f}% | src={metrics['source']}"
        )

        # --- DANGER ---
        if metrics["is_buying_climax"] or self.bingx_oi_delta < -1.5:
            if not self.is_short_active:
                self.is_short_active = True
                self.short_timer = 15
                logging.warning(
                    f"[{self.symbol}] ⚠️ DANGER: RVOL={rvol}x, "
                    f"OI Δ={self.bingx_oi_delta:+.2f}% — ищу фитиль для шорта"
                )
                await send_telegram(
                    f"⚠️ *[DANGER - SHORT REVERSAL]*\n"
                    f"Монета: `#{self.symbol.upper()}`\n"
                    f"RVOL 5M: `{rvol}x` (Пред: `{rvol_prev}x`)\n"
                    f"BingX OI Δ: `{self.bingx_oi_delta:+.2f}%`\n"
                    f"Ищу 1M фитиль для *ШОРТА* (15 минут)"
                )

        # --- SHORT по фитилю ---
        if self.is_short_active:
            if self.bingx_executor.validate_1m_pinbar(kline) and kline['close'] < ema7:
                stop_loss = round(kline['high'] * 1.001, 5)
                await self.bingx_executor.execute_market_short(
                    kline['close'], stop_loss
                )
                self.is_short_active = False
                return

            self.short_timer -= 1
            if self.short_timer <= 0:
                self.is_short_active = False
                logging.info(f"[{self.symbol}] Таймер DANGER истёк")

        # --- LONG ---
        if (
            metrics["is_healthy_trend"]
            and self.bingx_oi_delta > 0.5
            and not self.is_short_active
            and kline['close'] <= ema7
        ):
            await self.bingx_executor.execute_market_long(kline['close'])


# ==========================================
# 7. WEB + MAIN
# ==========================================

async def handle_ping(request):
    return web.Response(text="Bot is running!")


async def run_bot_safe(symbol, symbol_bingx):
    try:
        bot = QuasimodoBotCore(symbol=symbol, symbol_bingx=symbol_bingx)
        await bot.start()
    except Exception as e:
        logging.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        logging.error(traceback.format_exc())


async def main():
    app = web.Application()
    app.router.add_get("/", handle_ping)

    port = int(os.environ.get("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info(f"HTTP-сервер запущен на порту {port}")

    asyncio.create_task(run_bot_safe(SYMBOL, SYMBOL_BINGX))

    await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Бот остановлен пользователем.")
