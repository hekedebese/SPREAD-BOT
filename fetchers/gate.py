import aiohttp
import time
import logging

logger = logging.getLogger("bot")

CACHE_STATUS = {}
CACHE_TTL = 600  # 10 минут

# ---------------- ЦЕНЫ ----------------
async def get_gate_futures_prices():
    url = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                data = await resp.json()
                prices = {}
                for t in data:
                    if "last_price" in t:
                        symbol = t["name"].replace("_USDT", "")
                        prices[symbol] = float(t["last_price"])
                return prices
    except Exception as e:
        logger.warning(f"fetch_gate_prices failed: {e}")
        return {}

# ---------------- ДЕПОЗИТ/ВЫВОД ----------------
async def get_gate_status(symbol: str):
    """
    Возвращает (deposit, withdraw) для монеты на Gate.io (Spot)
    """
    now = time.time()
    if symbol in CACHE_STATUS and now - CACHE_STATUS[symbol][2] < CACHE_TTL:
        return CACHE_STATUS[symbol][0], CACHE_STATUS[symbol][1]

    url = "https://api.gateio.ws/api/v4/spot/currencies"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                data = await resp.json()
    except Exception as e:
        logger.warning(f"get_gate_status failed: {e}")
        return False, False

    deposit, withdraw = False, False
    for coin in data:
        if coin.get("currency", "").upper() == symbol.upper():
            deposit = not coin.get("deposit_disabled", True)
            withdraw = not coin.get("withdraw_disabled", True)
            break

    CACHE_STATUS[symbol] = (deposit, withdraw, now)
    return deposit, withdraw
