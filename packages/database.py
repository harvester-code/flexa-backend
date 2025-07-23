import asyncio
from typing import AsyncGenerator

import redshift_connector
from fastapi import HTTPException
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from supabase._async.client import AsyncClient, create_client

from packages.secrets import get_secret

# ============================================================
# NOTE: Redshift 연결을 위한 설정 (Refactored for production)
POOL_SIZE_MAP = {"development": 5, "production": 20}
POOL_SIZE = POOL_SIZE_MAP.get(get_secret("ENVIRONMENT"), 1)
TIMEOUT = 60

# Create a SQLAlchemy QueuePool for Redshift connections
redshift_pool = QueuePool(
    lambda: redshift_connector.connect(
        host=get_secret("REDSHIFT_HOST"),
        database=get_secret("REDSHIFT_DBNAME"),
        port=get_secret("REDSHIFT_PORT"),
        user=get_secret("REDSHIFT_USERNAME"),
        password=get_secret("REDSHIFT_PASSWORD"),
    ),
    max_overflow=10,  # Allow some overflow connections
    pool_size=POOL_SIZE,
    timeout=TIMEOUT,
)


async def validate_redshift_connection(conn):
    """Validate a Redshift connection by executing a simple query."""
    try:
        await asyncio.to_thread(conn.cursor().execute, "SELECT 1")
        return True
    except redshift_connector.Error as e:
        logger.error(f"Connection validation failed: {e}")
        return False


async def get_redshift_connection():
    conn = None
    try:
        # Retrieve a connection from the pool
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
# HACK: Supabase 클라이언트 생성 함수. 현재는 백엔드에서 안쓰이나, 추후 db 리팩토링 이후 필요없을시 삭제 예정
async def aget_supabase_client() -> AsyncClient:
    SUPABASE_PROJECT_URL: str = get_secret("SUPABASE_PROJECT_URL")
    SUPABASE_PUBLIC_KEY: str = get_secret("SUPABASE_PUBLIC_KEY")

    return await create_client(SUPABASE_PROJECT_URL, SUPABASE_PUBLIC_KEY)


async def aget_supabase_auth_client() -> AsyncClient:
    SUPABASE_PROJECT_URL: str = get_secret("SUPABASE_PROJECT_URL")
    SUPABASE_SECRET_KEY: str = get_secret("SUPABASE_SECRET_KEY")

    return await create_client(SUPABASE_PROJECT_URL, SUPABASE_SECRET_KEY)


# ============================================================
Base = declarative_base()
