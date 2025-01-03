import os

import sqlalchemy
from boto3 import client
from fastapi import HTTPException, status
from snowflake.sqlalchemy import URL
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


def get_snowflake_session():
    engine = sqlalchemy.create_engine(
        URL(
            account=os.getenv("SNOWFLAKE_ACCOUNT_IDENTIFIER"),
            user=os.getenv("SNOWFLAKE_USERNAME"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            database="CIRIUMSKY",
            schema="PUBLIC",
            warehouse="COMPUTE_WH",
        )
    )

    return engine


snowflake_engine = sqlalchemy.create_engine(
    URL(
        account=os.getenv("SNOWFLAKE_ACCOUNT_IDENTIFIER"),
        user=os.getenv("SNOWFLAKE_USERNAME"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database="CIRIUMSKY",
        schema="PUBLIC",
        warehouse="COMPUTE_WH",
    )
)

# supabase_url = sqlalchemy.engine.URL.create(
#     drivername="postgresql",
#     user="postgres",
#     password=os.getenv("SUPABASE_PASSWORD"),
#     host=os.getenv("SUPABASE_HOST"),
#     port=5432,
#     dbname="postgres",
# )

PASSWORD = os.getenv("SUPABASE_PASSWORD")
HOST = os.getenv("SUPABASE_HOST")
supabase_url = f"postgresql+psycopg2://postgres.notkalevegkwirkikgvx:{PASSWORD}@aws-0-ap-northeast-2.pooler.supabase.com:5432/postgres?sslmode=require"
supabase_engine = sqlalchemy.create_engine(supabase_url)


async def context_get_supabase_conn():
    conn = None
    try:

        conn = supabase_engine.connect()

        yield conn

    except SQLAlchemyError as e:
        print(e)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="DB 접속 에러"
        )

    finally:
        if conn:
            conn.close()
