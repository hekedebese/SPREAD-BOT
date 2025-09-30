import time
import hmac
import hashlib
import aiohttp
import logging
import os
from config import MEXC_API_KEY, MEXC_API_SECRET

logger = logging.getLogger("bot")

CACHE_STATUS = {}
CACHE_TTL = 600  # 10 минут

# --- SPOT ---
URL_SPOT = "https://api.mexc.com/api/v3/ticker/price"

async def get_mexc_spot_prices():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(URL_SPOT, timeout=10) as r:
                if r.status != 200:
                    logger.warning(f"[mexc_spot] bad status {r.status}")
                    return {}
                data = await r.json()
                return {
                    item["symbol"].upper(): float(item["price"])
                    for item in data
                    if "symbol" in item and "price" in item
                }
    except Exception as e:
        logger.warning(f"[mexc_spot] fetch failed: {e}")
        return {}

# --- FUTURES ---
URL_FUT = "https://contract.mexc.com/api/v1/contract/ticker"

async def get_mexc_futures_prices():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(URL_FUT, timeout=10) as r:
                if r.status != 200:
                    logger.warning(f"[mexc_fut] bad status {r.status}")
                    return {}
                data = await r.json()
                items = data.get("data", [])
                return {
                    item["symbol"].upper(): float(item["lastPrice"])
                    for item in items
                    if "symbol" in item and "lastPrice" in item
                }
    except Exception as e:
        logger.warning(f"[mexc_fut] fetch failed: {e}")
        return {}    

# ---------------- ДЕПОЗИТ/ВЫВОД ----------------
async def get_mexc_status(symbol: str):
    """
    Возвращает (deposit, withdraw) для монеты на MEXC.
    """
    now = time.time()
    key = ("mexc", symbol.upper())
    if key in CACHE_STATUS and now - CACHE_STATUS[key][2] < CACHE_TTL:
        return CACHE_STATUS[key][0], CACHE_STATUS[key][1]

    url = "https://api.mexc.com/api/v3/capital/config/getall"
    timestamp = int(time.time() * 1000)
    query = f"timestamp={timestamp}&recvWindow=5000"
    signature = hmac.new(MEXC_API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MEXC-APIKEY": MEXC_API_KEY}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{url}?{query}&signature={signature}", headers=headers, timeout=10) as resp:
                text = await resp.text()
                if resp.status != 200:
                    logger.warning(f"[mexc_status] {symbol} bad status {resp.status}, text={text}")
                    return False, False
                data = await resp.json()

        deposit, withdraw = False, False
        base = symbol.replace("USDT", "").upper()
        found = False
        for coin in data:
            if coin.get("coin", "").upper() == base:
                deposit = coin.get("enableDeposit", False)
                withdraw = coin.get("enableWithdraw", False)
                logger.info(f"[mexc_status] MATCH {base} -> deposit={deposit}, withdraw={withdraw}")
                found = True
                break

        if not found:
            sample = data[:3] if isinstance(data, list) else data
            logger.warning(f"[mexc_status] NO MATCH for {base}, sample={sample}")

        CACHE_STATUS[key] = (deposit, withdraw, now)
        return deposit, withdraw

    except Exception as e:
        logger.warning(f"get_mexc_status failed for {symbol}: {e}")
        return False, False