import os
from typing import AsyncGenerator

from fastapi import HTTPException, status
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from supabase._async.client import AsyncClient, create_client

# ============================================================
SNOWFLAKE_ENGINE = create_engine(
    URL(
        account=os.getenv("SNOWFLAKE_ACCOUNT_IDENTIFIER"),
        user=os.getenv("SNOWFLAKE_USERNAME"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database="CIRIUMSKY",
        schema="PUBLIC",
        warehouse="DEV_FLEXA_DEVELOPER_WH_S",
    )
)


def get_snowflake_session():
    conn = None
    try:
        conn = SNOWFLAKE_ENGINE.connect()
        yield conn
    except SQLAlchemyError as err:
        print(err)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="현재 요청하신 서비스 이용이 어려운 상태입니다.",
        )
    finally:
        if conn:
            conn.close()
    return SUPABASE_ENGINE


# ============================================================
# FIXME: https://supabase.com/docs/reference/python/initializing 참고해서 다시 작성하기
SUPABASE_USERNAME = os.getenv("SUPABASE_USERNAME")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_PORT = os.getenv("SUPABASE_PORT")
SUPABASE_DBNAME = os.getenv("SUPABASE_DBNAME")

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
    SUPABASE_PROJECT_URL: str = os.getenv("SUPABASE_PROJECT_URL")
    SUPABASE_PUBLIC_KEY: str = os.getenv("SUPABASE_PUBLIC_KEY")

    return await create_client(SUPABASE_PROJECT_URL, SUPABASE_PUBLIC_KEY)


async def aget_supabase_auth_client() -> AsyncClient:
    SUPABASE_PROJECT_URL: str = os.getenv("SUPABASE_PROJECT_URL")
    SUPABASE_SECRET_KEY: str = os.getenv("SUPABASE_SECRET_KEY")

    return await create_client(SUPABASE_PROJECT_URL, SUPABASE_SECRET_KEY)


# ============================================================
Base = declarative_base()
