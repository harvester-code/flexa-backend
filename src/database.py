import os
from typing import AsyncGenerator

import boto3
from boto3 import client
from fastapi import HTTPException, status
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from supabase._async.client import AsyncClient, create_client

# ============================================================
S3_SAVE_PATH = "s3://flexa-prod-ap-northeast-2-data-storage/simulations"


def get_s3_client():
    s3_client = client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name="ap-northeast-2",
    )
    return s3_client


def get_boto3_session() -> boto3.Session:
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name="ap-northeast-2",
    )
    return session


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
SUPABASE_USERNAME = os.getenv("SUPABASE_USERNAME")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_PORT = os.getenv("SUPABASE_PORT")
SUPABASE_DBNAME = os.getenv("SUPABASE_DBNAME")

SUPABASE_ENGINE = create_async_engine(
    f"postgresql+asyncpg://{SUPABASE_USERNAME}:{SUPABASE_PASSWORD}@{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DBNAME}",
    pool_size=5,
    max_overflow=2,
    pool_pre_ping=True,
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
        finally:
            await session.close()


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
