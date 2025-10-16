# db.py
import asyncio
import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List
from sqlalchemy import Column, Integer, String, DateTime, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker

logger = logging.getLogger("bot")

# --- ПУТЬ К БАЗЕ ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bot.db")
DATABASE_URL = f"sqlite+aiosqlite:///{DB_PATH}"

# --- БАЗА ---
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_recycle=1800,  # 💡 обновляем соединения каждые 30 минут
    future=True,
)

async_session = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

Base = declarative_base()

# --- МОДЕЛЬ ---
class User(Base):
    __tablename__ = "users"

    user_id = Column(Integer, primary_key=True)
    registered = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    trial_end = Column(DateTime(timezone=True), nullable=True)
    subscription = Column(Integer, default=0)  # 0/1
    filter_value = Column("filter", String, nullable=True)  # "5-12%" / "12-19%" / "19%+"
    notify = Column(Integer, default=1)  # 1=получать уведомления, 0=стоп


# --- ИНИЦИАЛИЗАЦИЯ ---
async def init_db():
    """Создание базы и включение WAL"""
    logger.info(f"Использую базу: {DB_PATH}")
    async with engine.begin() as conn:
        # включаем WAL режим для sqlite
        await conn.exec_driver_sql("PRAGMA journal_mode=WAL;")
        await conn.run_sync(Base.metadata.create_all)

        # ensure notify column exists
        try:
            res = await conn.exec_driver_sql("PRAGMA table_info('users')")
            cols = [row[1] for row in res.fetchall()]
            if 'notify' not in cols:
                await conn.exec_driver_sql("ALTER TABLE users ADD COLUMN notify INTEGER DEFAULT 1")
        except Exception as e:
            logger.warning(f"DB migration check failed: {e}")


# --- ОПЕРАЦИИ С ПОЛЬЗОВАТЕЛЯМИ ---
async def get_user(user_id: int) -> User | None:
    async with async_session() as session:
        return await session.get(User, user_id)


async def add_user(user_id: int, trial_days: int = 1) -> User:
    async with async_session() as session:
        now = datetime.now(timezone.utc)
        user = User(
            user_id=user_id,
            registered=now,
            trial_end=now + timedelta(days=trial_days),
            subscription=0,
            filter_value="5-12%",
        )
        session.add(user)
        await session.commit()
        return user


async def update_user(user_id: int, **kwargs) -> User | None:
    async with async_session() as session:
        user = await session.get(User, user_id)
        if not user:
            return None

        logger.info(f"[DB:update_user] user_id={user_id} incoming kwargs={kwargs!r}")

        # старое имя ключа filter → filter_value
        if "filter" in kwargs and "filter_value" not in kwargs:
            kwargs["filter_value"] = kwargs.pop("filter")

        # защита от случайных строк типа "⚙️ Фильтры"
        if "filter_value" in kwargs:
            fv = kwargs.get("filter_value")
            if isinstance(fv, str):
                fv_stripped = fv.strip()
                if "Фильтр" in fv_stripped or "Фильтры" in fv_stripped or "⚙" in fv_stripped:
                    logger.warning(f"[DB:update_user] Ignoring suspicious filter_value update for user {user_id}: {fv!r}")
                    kwargs.pop("filter_value", None)

        for key, value in kwargs.items():
            setattr(user, key, value)

        await session.commit()
        await upsert_user_in_cache(user)
        logger.info(f"[DB:update_user] user_id={user_id} updated with {kwargs!r}")
        return user


async def get_all_users() -> List[tuple[int, str]]:
    async with async_session() as session:
        stmt = select(User.user_id, User.filter_value)
        rows = (await session.execute(stmt)).all()
        return [(r.user_id, r.filter_value) for r in rows]


async def get_all_users_full() -> List[User]:
    async with async_session() as session:
        stmt = select(User)
        res = await session.execute(stmt)
        return list(res.scalars())


# --- КЭШ ПОЛЬЗОВАТЕЛЕЙ ---
USERS_CACHE: Dict[int, Dict[str, Any]] = {}


async def load_users_cache():
    """Полная загрузка всех пользователей в кэш"""
    global USERS_CACHE
    users = await get_all_users_full()

    logger.info(f"[DB] get_all_users_full вернул: {len(users)} строк")

    def parse_dt(val: Any) -> datetime | None:
        if not val:
            return None
        if isinstance(val, datetime):
            return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
        try:
            dt = datetime.fromisoformat(str(val))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None

    new_cache = {}
    for u in users:
        parsed_trial_end = parse_dt(u.trial_end)
        logger.info(
            f"[DB] user_id={u.user_id}, subscription={u.subscription}, "
            f"trial_end_raw={u.trial_end}, trial_end_parsed={parsed_trial_end}, filter={u.filter_value}"
        )
        new_cache[u.user_id] = {
            "subscription": int(u.subscription or 0),
            "trial_end": parsed_trial_end,
            "filter": u.filter_value or "5-12%",
            "notify": 1 if getattr(u, "notify", None) is None else int(u.notify),
        }

    USERS_CACHE.clear()
    USERS_CACHE.update(new_cache)
    logger.info(f"[DB] USERS_CACHE заполнен: {len(USERS_CACHE)} записей")


async def refresh_users_cache_periodically(interval_seconds: int = 300):
    """Периодическая перезагрузка кэша"""
    while True:
        try:
            await load_users_cache()
        except Exception:
            pass
        await asyncio.sleep(interval_seconds)


async def upsert_user_in_cache(user: User):
    USERS_CACHE[user.user_id] = {
        "subscription": int(user.subscription or 0),
        "trial_end": user.trial_end if user.trial_end and user.trial_end.tzinfo else (
            user.trial_end.replace(tzinfo=timezone.utc) if user.trial_end else None
        ),
        "filter": user.filter_value or "5-12%",
        "notify": 1 if getattr(user, "notify", None) is None else int(user.notify),
    }


def has_active_subscription(info: dict) -> bool:
    """Проверка, есть ли активная подписка или пробный период"""
    if not info:
        return False

    if info.get("subscription", 0) == 1:
        return True

    trial_end = info.get("trial_end")
    if trial_end and trial_end > datetime.now(timezone.utc):
        return True

    return False


# --- ДОБАВЛЕНО ---
async def get_users_count() -> int:
    """Возвращает количество пользователей в БД"""
    async with async_session() as session:
        stmt = select(User.user_id)
        res = await session.execute(stmt)
        return len(res.fetchall())