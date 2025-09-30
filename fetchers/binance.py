import aiohttp
import logging
import time
import hmac
import hashlib
import os
import asyncio
from config import BINANCE_API_KEY, BINANCE_API_SECRET

logger = logging.getLogger("bot")

# Futures
PRICE_URL_FUT = "https://fapi.binance.com/fapi/v1/ticker/price"
EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"

# Spot (для статусов депозита/вывода)
STATUS_URL = "https://api.binance.com/sapi/v1/capital/config/getall"

# --- CACHE ---
_trading_symbols_cache = None
_cache_timestamp = 0
CACHE_TTL = 600  # 10 минут
CACHE_STATUS = {}

# -------------------------------------------------
# Получение списка реально торгуемых фьючерсов
# -------------------------------------------------
async def get_trading_symbols():
    global _trading_symbols_cache, _cache_timestamp
    now = asyncio.get_event_loop().time()

    if _trading_symbols_cache is not None and (now - _cache_timestamp) < CACHE_TTL:
        return _trading_symbols_cache

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(EXCHANGE_INFO_URL, timeout=10) as r:
                if r.status != 200:
                    logger.warning(f"[binance_fut] exchangeInfo bad status {r.status}")
                    return set()
                data = await r.json()
                symbols = {
                    item["symbol"].upper()
                    for item in data.get("symbols", [])
                    if item.get("status") == "TRADING"
                }
                _trading_symbols_cache = symbols
                _cache_timestamp = now
                return symbols
    except Exception as e:
        logger.warning(f"[binance_fut] exchangeInfo fetch failed: {e}")
        return set()

# -------------------------------------------------
# Цены Binance Futures
# -------------------------------------------------
async def get_binance_futures_prices():
    try:
        trading_symbols = await get_trading_symbols()
        if not trading_symbols:
            logger.warning("[binance_fut] no trading symbols found")
            return {}

        async with aiohttp.ClientSession() as session:
            async with session.get(PRICE_URL_FUT, timeout=10) as r:
                if r.status != 200:
                    logger.warning(f"[binance_fut] ticker/price bad status {r.status}")
                    return {}
                data = await r.json()
                return {
                    item["symbol"].upper(): float(item["price"])
                    for item in data
                    if "symbol" in item and "price" in item and item["symbol"].upper() in trading_symbols
                }
    except Exception as e:
        logger.warning(f"[binance_fut] fetch failed: {e}")
        return {}

# -------------------------------------------------
# Статус депозит/вывод (Spot)
# -------------------------------------------------
async def get_binance_status(symbol: str):
    """
    Проверяет статус депозита/вывода для монеты (Spot).
    Возвращает (deposit_enabled: bool, withdraw_enabled: bool).
    """
    now = time.time()
    key = ("binance", symbol.upper())
    if key in CACHE_STATUS and now - CACHE_STATUS[key][2] < CACHE_TTL:
        return CACHE_STATUS[key][0], CACHE_STATUS[key][1]

    try:
        timestamp = int(time.time() * 1000)
        query = f"timestamp={timestamp}&recvWindow=5000"
        signature = hmac.new(
            BINANCE_API_SECRET.encode(),
            query.encode(),
            hashlib.sha256
        ).hexdigest()
        url = f"{STATUS_URL}?{query}&signature={signature}"
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=10) as resp:
                text = await resp.text()
                if resp.status != 200:
                    logger.warning(f"[binance_status] {symbol} bad status {resp.status}, text={text}")
                    return False, False
                data = await resp.json()

        deposit, withdraw = False, False
        base = symbol.replace("USDT", "").upper()
        found = False
        for coin in data:
            if coin.get("coin", "").upper() == base:
                deposit = coin.get("depositAllEnable", False)
                withdraw = coin.get("withdrawAllEnable", False)
                logger.info(f"[binance_status] MATCH {base} -> deposit={deposit}, withdraw={withdraw}")
                found = True
                break

        if not found:
            sample = data[:3] if isinstance(data, list) else data
            logger.warning(f"[binance_status] NO MATCH for {base}, sample={sample}")

        CACHE_STATUS[key] = (deposit, withdraw, now)
        return deposit, withdraw

    except Exception as e:
        logger.warning(f"get_binance_status failed for {symbol}: {e}")
        return False, False