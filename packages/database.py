import asyncio
import time
from typing import AsyncGenerator

import redshift_connector
from fastapi import HTTPException
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool


from packages.secrets import get_secret

# ============================================================
# NOTE: Redshift 연결을 위한 설정 (Refactored for production)
POOL_RECYCLE = 60 * 60 * 6  # 6 hours
POOL_SIZE_MAP = {"development": 5, "production": 20}
POOL_SIZE = POOL_SIZE_MAP.get(get_secret("ENVIRONMENT"), 1)
TIMEOUT = 60


# Redshift Connection
def redshift_connect():
    conn = redshift_connector.connect(
        host=get_secret("REDSHIFT_HOST"),
        database=get_secret("REDSHIFT_DBNAME"),
        port=get_secret("REDSHIFT_PORT"),
        user=get_secret("REDSHIFT_USERNAME"),
        password=get_secret("REDSHIFT_PASSWORD"),
    )
    conn._created_at = time.time()  # Save creation time
    return conn


# Recycle connection if it exceeds POOL_RECYCLE
def recycle_wrapper(conn):
    age = time.time() - getattr(conn, "_created_at", 0)
    if age > POOL_RECYCLE:
        logger.warning("Connection exceeded POOL_RECYCLE. Recycling entire pool.")
        try:
            conn.close()
        except Exception:
            pass
        # 🔥 Reset the entire pool
        try:
            redshift_pool.dispose()
        except Exception as e:
            logger.error(f"Error disposing pool: {e}")
        raise Exception("Expired connection recycled. Pool reset.")
    return conn


# Actual connection creator (includes recycle check)
def redshift_connect_recycled():
    conn = redshift_connect()
    return recycle_wrapper(conn)


# Create a SQLAlchemy QueuePool for Redshift connections
redshift_pool = QueuePool(
    redshift_connect,
    max_overflow=10,  # Allow some overflow connections (for temporary spikes)
    pool_size=POOL_SIZE,
    timeout=TIMEOUT,
)


# Validate Redshift connection
async def validate_redshift_connection(conn):
    """Validate a Redshift connection by executing a simple query."""
    try:
        await asyncio.to_thread(conn.cursor().execute, "SELECT 1")
        return True
    except redshift_connector.Error as e:
        logger.error(f"Connection validation failed: {e}")
        return False


# Get Redshift connection
async def get_redshift_connection():
    conn = None
    try:
        # Retrieve a connection from the pool
        conn = await asyncio.to_thread(redshift_pool.connect)
        try:
            # Recycle the connection if it exceeds POOL_RECYCLE
            recycle_wrapper(conn)

            logger.info("✅ Redshift connection recycled")
        except Exception:
            # Try to recreate the connection
            conn = await asyncio.to_thread(redshift_pool.connect)

        # Validate the connection
        if not await validate_redshift_connection(conn):
            conn.close()
            raise HTTPException(status_code=500, detail="Invalid Redshift connection")

        yield conn
    except Exception as e:
        logger.error(f"Error acquiring Redshift connection: {e}")
        raise HTTPException(
            status_code=503, detail="Error acquiring Redshift connection"
        )
    finally:
        if conn:
            try:
                conn.close()  # Return the connection to the pool
            except Exception as e:
                logger.error(f"Error returning Redshift connection to pool: {e}")


# ============================================================
# FIXME: https://supabase.com/docs/reference/python/initializing 참고해서 다시 작성하기
SUPABASE_USERNAME = get_secret("SUPABASE_USERNAME")
SUPABASE_PASSWORD = get_secret("SUPABASE_PASSWORD")
SUPABASE_HOST = get_secret("SUPABASE_HOST")
SUPABASE_PORT = get_secret("SUPABASE_PORT")
SUPABASE_DBNAME = get_secret("SUPABASE_DBNAME")

# HACK: 현재 간헐적으로 서버와 통신이 끊기는 상황, 코드상 풀은 제대로 연결되어있으나 정확한 해결방법을 찾지못해 임시로 풀사이즈와 pool_pre_ping을 추가함.
SUPABASE_ENGINE = create_async_engine(
    f"postgresql+asyncpg://{SUPABASE_USERNAME}:{SUPABASE_PASSWORD}@{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DBNAME}",
    pool_size=15,
    max_overflow=5,
    pool_pre_ping=True,
    connect_args={"timeout": 30, "command_timeout": 60},
)

AsyncSessionLocal = sessionmaker(
    bind=SUPABASE_ENGINE,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def aget_supabase_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        # finally:
        #     await session.close()


# ============================================================
Base = declarative_base()
