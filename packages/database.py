import asyncio
from queue import Empty, Queue
from threading import Lock
from typing import AsyncGenerator

import redshift_connector
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from supabase._async.client import AsyncClient, create_client

from packages.secrets import get_secret

# ============================================================
# NOTE: Redshift 연결을 위한 설정
POOL_SIZE_MAP = {"development": 5, "production": 20}
POOL_SIZE = POOL_SIZE_MAP.get(get_secret("ENVIRONMENT"), 1)
TIMEOUT = 60

redshift_connection_pool = Queue(maxsize=POOL_SIZE)
redshift_pool_lock = Lock()


def create_redshift_connection():
    return redshift_connector.connect(
        host=get_secret("REDSHIFT_HOST"),
        database=get_secret("REDSHIFT_DBNAME"),
        port=get_secret("REDSHIFT_PORT"),
        user=get_secret("REDSHIFT_USERNAME"),
        password=get_secret("REDSHIFT_PASSWORD"),
    )


def initialize_redshift_pool():
    with redshift_pool_lock:
        while not redshift_connection_pool.full():
            try:
                conn = create_redshift_connection()
                redshift_connection_pool.put(conn)
            except redshift_connector.Error as e:
                print(f"Error connecting to Redshift: {e}")
                break


async def get_redshift_connection():
    conn = None
    try:
        # Redshift 연결을 스레드에서 가져옴 (동기 드라이버 대응)
        conn = await asyncio.to_thread(
            redshift_connection_pool.get, True, timeout=TIMEOUT
        )

        # Health check to ensure the connection is valid
        try:
            await asyncio.to_thread(conn.cursor().execute, "SELECT 1")
        except redshift_connector.Error as health_check_error:
            print(f"Health check failed, reconnecting: {health_check_error}")
            conn = create_redshift_connection()

        yield conn
    except Empty:
        raise HTTPException(
            status_code=503, detail="Redshift connection pool exhausted"
        )
    finally:
        if conn:
            try:
                await asyncio.to_thread(redshift_connection_pool.put, conn)
            except Exception as e:
                print(f"Error returning Redshift connection to pool: {e}")
                pass  # 이미 풀 꽉 찬 경우


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
