import aiohttp
import time
import logging

logger = logging.getLogger("bot")

CACHE_STATUS = {}
CACHE_TTL = 600  # 10 минут

# ---------------- ЦЕНЫ ----------------
async def get_bingx_spot_prices():
    url = "https://open-api.bingx.com/openApi/spot/v1/ticker/price"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                data = await resp.json()
                prices = {}
                for t in data.get("data", []):
                    if t["symbol"].endswith("USDT") and t.get("trades"):
                        symbol = t["symbol"].replace("USDT", "")
                        price = t["trades"][0].get("price")
                        if price:
                            prices[symbol] = float(price)
                return prices
    except Exception as e:
        logger.warning(f"fetch_bingx_prices failed: {e}")
        return {}

# ---------------- ДЕПОЗИТ/ВЫВОД ----------------
async def get_bingx_status(symbol: str):
    """
    Возвращает (deposit, withdraw) для монеты на BingX Spot
    """
    now = time.time()
    if symbol in CACHE_STATUS and now - CACHE_STATUS[symbol][2] < CACHE_TTL:
        return CACHE_STATUS[symbol][0], CACHE_STATUS[symbol][1]

    url = "https://open-api.bingx.com/openApi/spot/v1/common/currencies"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                data = await resp.json()
    except Exception as e:
        logger.warning(f"get_bingx_status failed: {e}")
        return False, False

    deposit, withdraw = False, False
    for coin in data.get("data", []):
        if coin.get("asset", "").upper() == symbol.upper():
            deposit = coin.get("enableDeposit", False)
            withdraw = coin.get("enableWithdraw", False)
            break

    CACHE_STATUS[symbol] = (deposit, withdraw, now)
    return deposit, withdraw
