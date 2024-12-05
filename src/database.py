import os

from boto3 import client
from supabase import create_client


def supabase_public_clinet():

    url: str = os.environ.get("SUPABASE_URL")
    key: str = os.environ.get("SUPABASE_PUBLIC_KEY")
    supabase = create_client(url, key)

    return supabase


def supabase_auth_client():
    url: str = os.environ.get("SUPABASE_URL")
    key: str = os.environ.get("SUPABASE_SECRET_KEY")
    supabase = create_client(url, key)

    return supabase


def aws_s3_client():
    s3_client = client(
        "s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name="ap-northeast-2",
    )

    return s3_client
