# main.py (исправленный — полный)
import logging
import asyncio
import random
import time
from datetime import datetime, timezone, timedelta
from logging.handlers import TimedRotatingFileHandler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telebot.async_telebot import AsyncTeleBot
from telebot import types
from collections import defaultdict
import maintenance
from db import (
    init_db, get_user, add_user, update_user,
    USERS_CACHE, load_users_cache, refresh_users_cache_periodically,
    upsert_user_in_cache, has_active_subscription
)
from config import TELEGRAM_TOKEN, SPREAD_THRESHOLD_PERCENT, PRICE_CHECK_INTERVAL, ADMINS, ALERT_COOLDOWN_MINUTES, SPREAD_CHANGE_THRESHOLD
from utils import normalize_and_match, load_exceptions
from fetchers.mexc import get_mexc_spot_prices, get_mexc_futures_prices, get_mexc_status
from fetchers.bitget import get_bitget_futures_prices, get_bitget_status
from fetchers.bingx import get_bingx_spot_prices, get_bingx_status
from fetchers.gate import get_gate_futures_prices, get_gate_status
from fetchers.binance import get_binance_futures_prices, get_trading_symbols, get_binance_status

# ---------- ЛОГИ (ежедневная ротация, 7 дней) ----------
log_formatter = logging.Formatter(
    fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logging.basicConfig(level=logging.INFO, force=True)
handler = TimedRotatingFileHandler(
    filename="bot.log",
    when="midnight",
    interval=1,
    backupCount=7,
    encoding="utf-8",
    utc=True
)
handler.setFormatter(log_formatter)
root_logger = logging.getLogger()
root_logger.addHandler(handler)
# приглушаем слишком болтливые библиотеки
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("telebot").setLevel(logging.WARNING)
logger = logging.getLogger("bot")

def ensure_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if not isinstance(dt, datetime):
        try:
            dt = datetime.fromisoformat(str(dt))
        except Exception:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

# ---------- БОТ ----------
bot = AsyncTeleBot(TELEGRAM_TOKEN)
# ограничим параллельные отправки к Telegram (безопасность)
SEND_SEMAPHORE = asyncio.Semaphore(20)

# ---------- РЫНКИ ----------
MARKETS = {
    "mexc_spot": {"type": "spot", "fetch": get_mexc_spot_prices},
    "mexc_fut": {"type": "fut",  "fetch": get_mexc_futures_prices},
    "bitget_fut": {"type": "fut", "fetch": get_bitget_futures_prices},
    "gate_fut": {"type": "fut",   "fetch": get_gate_futures_prices},
    "binance_fut": {"type": "fut","fetch": get_binance_futures_prices},
    "bingx_spot": {"type": "spot","fetch": get_bingx_spot_prices},
}

EXCHANGE_NAMES = {
    "mexc_spot": "MEXC Spot",
    "mexc_fut": "MEXC Futures",
    "bitget_fut": "Bitget Futures",
    "gate_fut": "Gate Futures",
    "binance_fut": "Binance Futures",
    "bingx_spot": "BingX Spot",
}

# ---------- СТАТУСЫ ДЕПОЗИТ/ВЫВОД ----------
EXCHANGE_STATUS_FUNCS = {
    "bitget_fut": get_bitget_status,
    "gate_fut": get_gate_status,
    "bingx_spot": get_bingx_status,
    "binance_fut": get_binance_status,
    "mexc_spot": get_mexc_status,
    "mexc_fut": get_mexc_status,
}

async def get_symbol_status(exchange_key: str, symbol: str):
    func = EXCHANGE_STATUS_FUNCS.get(exchange_key)
    if not func:
        return False, False
    try:
        return await func(symbol)
    except Exception as e:
        logger.warning(f"get_symbol_status failed for {exchange_key} {symbol}: {e}")
        return False, False


# ---------- ВСПОМОГАТЕЛЬНЫЕ ----------
def main_menu_markup():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton("🔍 Проверить монету"))
    markup.add(types.KeyboardButton("⚙️ Фильтры"))
    markup.add(types.KeyboardButton("👤 Профиль"))
    markup.add(types.KeyboardButton("ℹ️ О боте"))
    return markup

async def show_main_menu(chat_id: int):
    await bot.send_message(chat_id, "Выберите действие:", reply_markup=main_menu_markup())

def passes_filter(spread: float, user_filter: str | None) -> bool:
    if user_filter == "5-12%":
        return 5 <= spread < 12
    elif user_filter == "12-19%":
        return 12 <= spread < 19
    elif user_filter == "19%+":
        return spread >= 19
    return spread >= SPREAD_THRESHOLD_PERCENT

async def has_access(user_id: int) -> bool:
    # админы всегда имеют доступ
    if user_id in ADMINS:
        return True

    info = USERS_CACHE.get(user_id)
    if not info:
        return False
    if int(info.get("subscription") or 0) == 1:
        return True
    trial_end = ensure_utc(info.get("trial_end"))
    if trial_end:
        now = datetime.now(timezone.utc)
        return now <= trial_end
    return False

async def remaining_trial_text(user_id: int) -> str:
    user = await get_user(user_id)
    if not user:
        return "❌ Пользователь не найден"

    await upsert_user_in_cache(user)

    if int(user.subscription or 0) == 1:
        return ""

    trial_end = ensure_utc(user.trial_end)
    if trial_end:
        now = ensure_utc(datetime.now(timezone.utc))
        if now <= trial_end:
            left = trial_end - now
            total = int(left.total_seconds())
            days = total // 86400
            hours = (total % 86400) // 3600
            minutes = (total % 3600) // 60
            return f"⏳ Бесплатный доступ: {days} дн {hours} ч {minutes} мин"

    return "❌ Бесплатный период истёк"

async def safe_send(user_id: int, text: str, **kwargs):
    """Отправка сообщения с семафором, без поднятия исключения наружу."""
    try:
        async with SEND_SEMAPHORE:
            await bot.send_message(user_id, text, **kwargs)
    except Exception as e:
        logger.warning(f"send_message to {user_id} failed: {e}")

# ---------- ПОЛУЧЕНИЕ ЦЕН ----------
async def safe_fetch(name, func):
    try:
        data = await func()
        return name, data
    except Exception as e:
        logger.warning(f"[WARN] Ошибка при получении данных с {name}: {e}")
        return name, None

async def fetch_all_prices():
    """
    Возвращает словарь {source_name: prices_dict} - где prices_dict может быть пустым {}.
    Важно: используем фильтр result is not None (None значит ошибка), пустой dict - допустимо.
    """
    tasks = [safe_fetch(name, info["fetch"]) for name, info in MARKETS.items()]
    results = await asyncio.gather(*tasks)
    return {name: result for name, result in results if result is not None}

def filter_valid_symbols(prices: dict):
    """
    Возвращает только те символы, для которых:
      - есть >=1 futures и >=1 spot (spot ↔ fut)
      OR
      - есть >=2 futures (fut ↔ fut)
    Пропускаем spot↔spot.
    """
    filtered = {}
    for symbol, markets in prices.items():
        types_map = {}
        for src in markets.keys():
            t = MARKETS.get(src, {}).get("type")
            types_map.setdefault(t, []).append(src)

        futures_sources = types_map.get("fut", [])
        spot_sources = types_map.get("spot", [])

        # fut-fut
        if len(futures_sources) >= 2:
            filtered[symbol] = markets
            continue
        # spot-fut
        if len(futures_sources) >= 1 and len(spot_sources) >= 1:
            filtered[symbol] = markets
            continue
        # остальные (например только spot) — пропускаем
    return filtered

# ---------- ЛОГ СПРЕДОВ (опционально) ----------
LOG_FILE = "spreads_log.txt"

async def log_random_spreads_to_file(prices: dict, sample_size: int = 30):
    try:
        if not prices:
            with open(LOG_FILE, "w", encoding="utf-8") as f:
                f.write("Нет доступных монет для проверки.\n")
            return
        symbols = list(prices.keys())
        if len(symbols) > sample_size:
            symbols = random.sample(symbols, sample_size)
        lines = [f"📊 Лог {len(symbols)} случайных монет:"]
        for symbol in symbols:
            data = prices[symbol]
            sources = list(data.keys())
            lines.append(f"\n{symbol}:")
            for i in range(len(sources)):
                for j in range(i + 1, len(sources)):
                    src1, src2 = sources[i], sources[j]
                    t1, t2 = MARKETS[src1]["type"], MARKETS[src2]["type"]
                    if t1 == "spot" and t2 == "spot":
                        continue
                    p1, p2 = data[src1], data[src2]
                    if not p1 or not p2:
                        continue
                    spread = abs(p1 - p2) / ((p1 + p2) / 2) * 100
                    lines.append(f"{EXCHANGE_NAMES.get(src1, src1)} vs {EXCHANGE_NAMES.get(src2, src2)}: {spread:.2f}%")
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
    except Exception as e:
        logger.warning(f"write log failed: {e}")

# ---------- АЛЕРТЫ ----------
LAST_ALERTS = {}  # {user_id: {symbol: datetime}}

# ---------- АЛЕРТЫ (с расширенным логированием) ----------
async def _check_and_alert():
    logger.info("START _check_and_alert")
    start_time = time.time()
    total_pairs = 0
    sent_count = 0
    max_spread = 0.0
    top_spreads = []  # list of tuples (spread, symbol, src1, src2, p1, p2)

    try:
        raw = await fetch_all_prices()
        try:
            loaded_counts = {k: (len(v) if v else 0) for k, v in raw.items()}
        except Exception:
            loaded_counts = {}
        logger.info(f"Загружено сырых данных: {sum(loaded_counts.values())} монет c {len(raw)} источников; детали: {loaded_counts}")

        try:
            prices = normalize_and_match(raw)
            logger.info(f"После нормализации: {len(prices)} монет")
        except Exception as e:
            logger.warning(f"normalize_and_match failed: {e}")
            prices = {}

        prices = filter_valid_symbols(prices)
        logger.info(f"После фильтрации (подходящие пары рынков): {len(prices)} монет")

        if not prices:
            logger.info("No valid prices found")
            return

        await log_random_spreads_to_file(prices)

        try:
            users_snapshot = list(USERS_CACHE.items())
            logger.info(f"USERS_CACHE size: {len(USERS_CACHE)}")
            for uid, uinfo in users_snapshot[:10]:
                logger.info(f"USERS_CACHE sample -> {uid}: {uinfo}")
        except Exception as e:
            logger.warning(f"Cannot dump USERS_CACHE: {e}")

        send_tasks = []
        url_map = {
            "mexc_spot": "https://www.mexc.com/exchange/{symbol}_USDT",
            "binance_fut": "https://www.binance.com/en/futures/{symbol}USDT",
            "bitget_fut": "https://www.bitget.com/en/futures/{symbol}USDT",
            "gate_fut": "https://www.gate.com/en/futures/USDT/{symbol}_USDT",
            "bingx_spot": "https://bingx.com/spot/{symbol}USDT",
            "mexc_fut": "https://www.mexc.com/futures/{symbol}_USDT?type=linear_swap",
        }

        for symbol, data in prices.items():
            sources = list(data.keys())
            for i in range(len(sources)):
                for j in range(i + 1, len(sources)):
                    src1, src2 = sources[i], sources[j]
                    t1, t2 = MARKETS[src1]["type"], MARKETS[src2]["type"]
                    if t1 == "spot" and t2 == "spot":
                        continue
                    p1, p2 = data[src1], data[src2]
                    if not p1 or not p2:
                        continue

                    spread = abs(p1 - p2) / ((p1 + p2) / 2) * 100
                    total_pairs += 1
                    if spread > max_spread:
                        max_spread = spread
                    top_spreads.append((spread, symbol, src1, src2, p1, p2))

                    if spread < SPREAD_THRESHOLD_PERCENT:
                        continue

                    for user_id, info in list(USERS_CACHE.items()):
                        try:
                            access = await has_access(user_id)
                        except Exception as e:
                            logger.warning(f"has_access failed for user {user_id}: {e}")
                            access = False

                        if not access:
                            logger.debug(f"[{symbol}] user {user_id} — доступ нет")
                            continue

                        if int(info.get("notify", 1)) == 0:
                            logger.debug(f"[{symbol}] user {user_id} — notify=0, пропуск")
                            continue

                        user_filter = info.get("filter")
                        if not passes_filter(spread, user_filter):
                            logger.debug(f"[{symbol}] user {user_id} — фильтр не прошёл ({user_filter})")
                            continue

                        if "collected_spreads" not in info:
                            info["collected_spreads"] = {}
                        if symbol not in info["collected_spreads"]:
                            info["collected_spreads"][symbol] = []
                        info["collected_spreads"][symbol].append({
                            "spread": spread,
                            "src1": src1,
                            "src2": src2,
                            "p1": p1,
                            "p2": p2,
                            "url1": url_map[src1].format(symbol=symbol),
                            "url2": url_map[src2].format(symbol=symbol),
                        })

            # --- отправка сообщений ---
            for user_id, info in list(USERS_CACHE.items()):
                if int(info.get("notify", 1)) == 0:
                    continue

                if "collected_spreads" in info and symbol in info["collected_spreads"]:
                    entries = info["collected_spreads"][symbol]
                    spread_percent = entries[0]["spread"]

                    now = datetime.now(timezone.utc)
                    last_alert = LAST_ALERTS.get(user_id, {}).get(symbol)
                    should_send = True

                    if last_alert:
                        last_time, last_spread = last_alert
                        if (now - last_time) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                            should_send = False
                        elif last_spread is not None and abs(spread_percent - last_spread) < SPREAD_CHANGE_THRESHOLD:
                            should_send = False

                    if should_send:
                        text = f"Монета: <b>{symbol}</b>\n\n"
                        for e in entries:
                            dep1, wdr1 = await get_symbol_status(e["src1"], symbol)
                            dep2, wdr2 = await get_symbol_status(e["src2"], symbol)

                            text += (
                                f'<a href="{e["url1"]}">{EXCHANGE_NAMES.get(e["src1"], e["src1"])}</a> 🔄 '
                                f'<a href="{e["url2"]}">{EXCHANGE_NAMES.get(e["src2"], e["src2"])}</a>\n'
                                f'📈 Спред: {e["spread"]:.2f}%\n'
                                f'💰 {EXCHANGE_NAMES.get(e["src1"], e["src1"])}: {e["p1"]:.6f}\n'
                                f'💰 {EXCHANGE_NAMES.get(e["src2"], e["src2"])}: {e["p2"]:.6f}\n\n'
                                f'Депозит: {"✅" if dep1 or dep2 else "❌"}\n'
                                f'Вывод: {"✅" if wdr1 or wdr2 else "❌"}\n\n'
                            )

                        text += f"🔖 Тикер: <code>{symbol}</code>"

                        send_tasks.append(
                            safe_send(
                                user_id,
                                text,
                                parse_mode="HTML",
                                disable_web_page_preview=True
                            )
                        )
                        sent_count += 1
                        if user_id not in LAST_ALERTS:
                            LAST_ALERTS[user_id] = {}
                        LAST_ALERTS[user_id][symbol] = (now, spread_percent)
                        logger.info(f"[{symbol}] user {user_id} — отправлено сообщение с {len(entries)} спредами")
                    else:
                        logger.debug(f"[{symbol}] user {user_id} — кулдаун или изменение < {SPREAD_CHANGE_THRESHOLD}% → пропуск")

                    info["collected_spreads"].pop(symbol, None)

        if sent_count == 0:
            try:
                top_spreads_sorted = sorted(top_spreads, key=lambda x: x[0], reverse=True)[:20]
                logger.info("Top spreads (top 20) — возможно причины неполучения уведомлений:")
                for s, sym, a, b, p1, p2 in top_spreads_sorted:
                    logger.info(f"{sym} {a} vs {b}: {s:.2f}% — {p1:.6f} / {p2:.6f}")
                logger.info(f"Максимальный спред в проходе: {max_spread:.2f}%")
            except Exception as e:
                logger.warning(f"Failed to log top spreads: {e}")

        if send_tasks:
            results = await asyncio.gather(*send_tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception):
                    logger.warning(f"Error in send task: {res}")

    except Exception as e:
        logger.exception(f"_check_and_alert failed: {e}")
    finally:
        elapsed = round(time.time() - start_time, 2)
        logger.info(f"END _check_and_alert | пар проверено: {total_pairs}, уведомлений поставлено в очередь: {sent_count}, время: {elapsed} сек.")


# ---------- ХЭНДЛЕРЫ ----------
user_states = {}  # {user_id: "waiting_for_coin"}

@bot.message_handler(commands=["start"])
async def handle_start(message):
    user_id = message.chat.id

    # если пользователя нет в БД — добавляем
    user = await get_user(user_id)
    if not user:
        user = await add_user(user_id)

    # если админ — всегда ставим подписку = 1
    if user_id in ADMINS:
        await update_user(user_id, subscription=1)

    # обновляем кэш пользователей (важно всегда после изменений)
    await load_users_cache()

    # ЛОГИРОВАНИЕ USERS_CACHE после запуска команды /start
    try:
        users_snapshot = list(USERS_CACHE.items())
        logger.info(f"[/start] USERS_CACHE после запуска команды: {len(USERS_CACHE)} записей.")
        for uid, uinfo in users_snapshot[:10]:
            logger.info(f"[/start] sample -> {uid}: {uinfo}")
    except Exception as e:
        logger.warning(f"[/start] Cannot dump USERS_CACHE: {e}")
    await bot.send_message(
        user_id,
        "Привет! 👋\nЯ отслеживаю спреды криптомонет.\nПервый день бесплатный!"
    )
    await show_main_menu(user_id)

@bot.message_handler(func=lambda m: True)
async def main_menu_handler(message):
    user_id = message.chat.id
    text = (message.text or "").strip()
    logger.warning(f"[DEBUG MESSAGE FLOW] user_id={user_id}, text='{text}', repr={repr(text)}")

    # --- команда /approve (для админов) ---
    if text.startswith("/approve"):
        if user_id not in ADMINS:
            await bot.send_message(user_id, "⛔ У вас нет прав для этой команды.")
            return
        args = text.split()
        if len(args) < 2:
            await bot.send_message(user_id, "Использование: /approve <user_id>")
            return
        try:
            target_id = int(args[1])
            new_trial = datetime.now(timezone.utc) + timedelta(days=30)
            await update_user(target_id, subscription=1, trial_end=new_trial)

            USERS_CACHE[target_id] = {
                **USERS_CACHE.get(target_id, {}),
                "subscription": 1,
                "trial_end": new_trial,
            }

            # сообщение админу
            await bot.send_message(
                user_id,
                f"✅ Подписка пользователю {target_id} активирована до "
                f"{new_trial.strftime('%Y-%m-%d %H:%M:%S UTC')}"
            )

            # сообщение пользователю
            try:
                await bot.send_message(
                    target_id,
                    f"✅ Ваша подписка активирована до "
                    f"{new_trial.strftime('%Y-%m-%d %H:%M:%S UTC')}"
                )
            except Exception:
                pass
        except ValueError:
            await bot.send_message(user_id, "user_id должен быть числом")
        return  # выходим сразу, не идём дальше

    # --- если ждём монету ---
    if user_states.get(user_id) == "waiting_for_coin":
        symbol_query = text.upper()
        user_states[user_id] = None
        await check_symbol_prices(user_id, symbol_query)
        return

    # пункты меню доступные без подписки
    allowed_wo_access = {"👤 Профиль", "ℹ️ О боте"}

    if text not in allowed_wo_access and text not in {
        "🔍 Проверить монету", 
        "⚙️ Фильтры", 
        "⬅️ Назад в меню",
        "5-12%", "12-19%", "19%+",
        "⛔ Отключить уведомления", "▶️ Включить уведомления",
        "💳 Оплатить", "✅ Я оплатил"
    }:
        await show_main_menu(user_id)
        return

    if text == "🔍 Проверить монету":
        if not await has_access(user_id):
            await bot.send_message(user_id, "⛔ Доступ ограничен. Купите подписку, чтобы продолжить пользоваться ботом.")
            return
        user_states[user_id] = "waiting_for_coin"
        await bot.send_message(user_id, "Введите название монеты (например, TRUMP):")

    elif text == "⚙️ Фильтры":
        if not await has_access(user_id):
            await bot.send_message(user_id, "⛔ Доступ ограничен. Купите подписку, чтобы продолжить пользоваться ботом.")
            return
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        markup.add(types.KeyboardButton("5-12%"), types.KeyboardButton("12-19%"), types.KeyboardButton("19%+"))
        markup.add(types.KeyboardButton("⬅️ Назад в меню"))
        await bot.send_message(user_id, "Выберите фильтр спреда:", reply_markup=markup)
        return

    elif text in {"5-12%", "12-19%", "19%+"}:
        new_filter = text.strip()
        logger.info(f"[ФИЛЬТР DEBUG] user_id={user_id} выбрал фильтр {new_filter}")
        await update_user(user_id, filter=new_filter)
        await load_users_cache()
        logger.info(f"[ФИЛЬТР DEBUG] USERS_CACHE[{user_id}] -> {USERS_CACHE.get(user_id)}")
        await bot.send_message(user_id, f"✅ Фильтр спреда установлен: {new_filter}")
        await show_main_menu(user_id)

    elif text == "👤 Профиль":
        user = await get_user(user_id)
        if not user:
            await bot.send_message(user_id, "Я вас не нашёл в базе. Нажмите /start")
            return
        sub_status = "не активна ❌"
        trial_end = ensure_utc(getattr(user, "trial_end", None))
        if (user.subscription or 0) == 1:
            if trial_end:
                trial_end_str = trial_end.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                sub_status = f"активна ✅ (до {trial_end_str})"
            else:
                sub_status = "активна ✅"
        else:
            trial_info = await remaining_trial_text(user_id)
            if trial_info:
                sub_status += f"\n{trial_info}"

        reg_date = ensure_utc(user.registered or datetime.now(timezone.utc)).strftime("%Y-%m-%d %H:%M:%S UTC")

        lines = [
            "📋 Профиль:",
            f"Дата регистрации: {reg_date}",
            f"Подписка: {sub_status}",
            f"Фильтр: {user.filter_value or 'по умолчанию'}"
        ]

        notif_raw = getattr(user, 'notify', None)
        notif = 1 if notif_raw is None else int(notif_raw)
        lines.append('Уведомления: ✅ включены' if notif == 1 else 'Уведомления: ⛔ отключены')

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        toggle_text = "⛔ Отключить уведомления" if notif == 1 else "▶️ Включить уведомления"
        markup.add(types.KeyboardButton(toggle_text))
        markup.add(types.KeyboardButton("💳 Оплатить"))
        markup.add(types.KeyboardButton("⬅️ Назад в меню"))

        await bot.send_message(user_id, "\n".join(lines), reply_markup=markup)

    elif text in {"⛔ Отключить уведомления", "▶️ Включить уведомления"}:
        user = await get_user(user_id)
        cur_raw = getattr(user, 'notify', None)
        current = 1 if cur_raw is None else int(cur_raw)
        new_val = 0 if text.startswith("⛔") else 1
        if new_val != current:
            await update_user(user_id, notify=new_val)
            await load_users_cache()
        state = "отключены ⛔" if new_val == 0 else "включены ✅"
        await bot.send_message(user_id, f"Уведомления {state}. Можно изменить в Профиле.")

    elif text == "💳 Оплатить":
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        markup.add(types.KeyboardButton("✅ Я оплатил"))
        markup.add(types.KeyboardButton("⬅️ Назад в меню"))
        await bot.send_message(
            user_id,
            "💳 Для оплаты переведите 40 USDT (TRC20) на кошелёк:\n\n"
            "<code>TMQPxZoa5yeghDPTUi3feLrcCCzC4ZkZuc</code>\n\n"
            "После оплаты нажмите «✅ Я оплатил».",
            reply_markup=markup,
            parse_mode="HTML"
        )

    elif text == "✅ Я оплатил":
        # Отправляем пользователю уведомление
        await bot.send_message(
            user_id,
            "✅ Ваш запрос на активацию подписки отправлен администратору.\n"
            "Ожидайте подтверждения, обычно это занимает несколько минут."
        )
        await show_main_menu(user_id)

        # Уведомляем всех админов
        # Уведомляем только тебя (без цикла)
        try:
            await bot.send_message(
                1879112903,  # <-- твой Telegram ID
                f"💳 Пользователь {user_id} ({message.from_user.first_name}) "
                f"нажал «Я оплатил». Проверь оплату и, если всё ок, активируй подписку:\n\n"
                f"<code>/approve {user_id}</code>",
                parse_mode="HTML"
            )
        except Exception:
            pass


    elif text == "ℹ️ О боте":
        await bot.send_message(user_id, "ℹ️ Я отслеживаю спреды между спотом и фьючерсами разных бирж.")
        await show_main_menu(user_id)

    elif text == "⬅️ Назад в меню":
        await show_main_menu(user_id)

    else:
        await show_main_menu(user_id)


# вспомогательная функция: обработка синхронного вызова проверки монеты
def process_check_symbol(message):
    symbol_query = (message.text or "").strip().upper()
    asyncio.create_task(check_symbol_prices(message.chat.id, symbol_query))

async def check_symbol_prices(chat_id: int, symbol_query: str):
    if not await has_access(chat_id):
        await bot.send_message(chat_id, "⛔ Доступ ограничен. Купите подписку, чтобы продолжить пользоваться ботом.")
        return

    raw_prices = await fetch_all_prices()
    prices = normalize_and_match(raw_prices)
    prices = filter_valid_symbols(prices)

    matches = {k: v for k, v in prices.items() if k == symbol_query}
    if not matches:
        exceptions = load_exceptions()
        # если в exceptions прописано сопоставление символа
        if symbol_query in exceptions and symbol_query in prices:
            matches[symbol_query] = prices[symbol_query]

    if not matches:
        await bot.send_message(chat_id, f"❌ Монета {symbol_query} не найдена или не имеет нужных рынков (spot/fut).")
        return

    # формируем подробное сообщение
    for symbol, data in matches.items():
        msg_lines = [f"📊 <b>{symbol}</b>"]

    # Цены
    for src in data.keys():
        msg_lines.append(f"<b>{EXCHANGE_NAMES.get(src, src)}:</b> {data[src]:.3f}")

    # Заголовок спредов
    msg_lines.append("\n💹 <b>СПРЕДЫ</b> 💹")

    sources = list(data.keys())
    for i in range(len(sources)):
        for j in range(i + 1, len(sources)):
            src1, src2 = sources[i], sources[j]
            t1, t2 = MARKETS[src1]["type"], MARKETS[src2]["type"]
            if t1 == "spot" and t2 == "spot":
                continue
            p1, p2 = data[src1], data[src2]
            if not p1 or not p2:
                continue
            spread = abs(p1 - p2) / ((p1 + p2) / 2) * 100
            msg_lines.append(
                f"{EXCHANGE_NAMES.get(src1, src1)} vs {EXCHANGE_NAMES.get(src2, src2)}: <b>{spread:.2f}%</b>"
            )

    # Тикер (копируемый)
    msg_lines.append(f"\n🔖 Тикер для копирования: <code>{symbol}</code>")

    await bot.send_message(chat_id, "\n".join(msg_lines), parse_mode="HTML")


# ---------- ЗАПУСК ----------
async def main():
    await init_db()

    # загружаем пользователей в кэш
    await load_users_cache()

    # ЛОГИРОВАНИЕ USERS_CACHE сразу после загрузки при старте
    try:
        users_snapshot = list(USERS_CACHE.items())
        logger.info(f"[main] USERS_CACHE загружен: {len(USERS_CACHE)} записей.")
        for uid, uinfo in users_snapshot[:10]:
            logger.info(f"[main] sample -> {uid}: {uinfo}")
    except Exception as e:
        logger.warning(f"[main] Cannot dump USERS_CACHE: {e}")
    # включаем подписку админам
    for admin_id in ADMINS:
        await update_user(admin_id, subscription=1)

    # периодическое обновление кэша (в фоне)
    asyncio.create_task(refresh_users_cache_periodically(300))

    # планировщик автоуведомлений
    scheduler = AsyncIOScheduler(timezone=timezone.utc)
    scheduler.add_job(
        _check_and_alert,
        "interval",
        seconds=PRICE_CHECK_INTERVAL,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=30,
        id="check_and_alert"
    )
    await maintenance.start_background_tasks(bot)
    scheduler.start()

    logger.info("🤖 Бот запущен.")
    await bot.infinity_polling(skip_pending=True)

if __name__ == "__main__":
    asyncio.run(main())