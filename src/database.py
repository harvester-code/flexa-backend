import os

from boto3 import client
from fastapi import status
from fastapi.exceptions import HTTPException
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from supabase import create_client


def supabase_public_clinet():
    url: str = os.getenv("SUPABASE_URL")
    key: str = os.getenv("SUPABASE_PUBLIC_KEY")
    supabase = create_client(url, key)

    return supabase


def supabase_auth_client():
    url: str = os.getenv("SUPABASE_URL")
    key: str = os.getenv("SUPABASE_SECRET_KEY")
    supabase = create_client(url, key)

    return supabase


def aws_s3_client():
    s3_client = client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name="ap-northeast-2",
    )
    return s3_client


# ============================================================
SNOWFLAKE_ENGINE = create_engine(
    URL(
        account=os.getenv("SNOWFLAKE_ACCOUNT_IDENTIFIER"),
        user=os.getenv("SNOWFLAKE_USERNAME"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database="CIRIUMSKY",
        schema="PUBLIC",
        warehouse="COMPUTE_WH",
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
