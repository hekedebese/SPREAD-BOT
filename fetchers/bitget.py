import aiohttp
import time
import logging

logger = logging.getLogger("bot")

CACHE_STATUS = {}
CACHE_TTL = 600  # 10 минут

# ---------------- ЦЕНЫ ----------------
async def get_bitget_futures_prices():
    url = "https://api.bitget.com/api/mix/v1/market/tickers?productType=umcbl"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                data = await resp.json()
                tickers = data.get("data", [])
                prices = {}
                for t in tickers:
                    symbol = t["symbol"].replace("_UMCBL", "")
                    prices[symbol] = float(t["last"])
                return prices
    except Exception as e:
        logger.warning(f"fetch_bitget_prices failed: {e}")
        return {}

# ---------------- ДЕПОЗИТ/ВЫВОД ----------------
async def get_bitget_status(symbol: str):
    """
    Возвращает (deposit, withdraw) для монеты на Bitget (Spot)
    """
    now = time.time()
    if symbol in CACHE_STATUS and now - CACHE_STATUS[symbol][2] < CACHE_TTL:
        return CACHE_STATUS[symbol][0], CACHE_STATUS[symbol][1]

    url = "https://api.bitget.com/api/spot/v1/public/currencies"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                data = await resp.json()
    except Exception as e:
        logger.warning(f"get_bitget_status failed: {e}")
        return False, False

    deposit, withdraw = False, False
    for coin in data.get("data", []):
        if coin.get("coinName", "").upper() == symbol.upper():
            deposit = coin.get("depositStatus", "false") == "true"
            withdraw = coin.get("withdrawStatus", "false") == "true"
            break

    CACHE_STATUS[symbol] = (deposit, withdraw, now)
    return deposit, withdraw
