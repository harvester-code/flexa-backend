import os

import boto3
from botocore.exceptions import ClientError


def get_s3_client():
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name="ap-northeast-2",
    )
    return s3_client


def check_s3_object_exists(bucket_name: str, object_key: str) -> bool:
    s3_client = get_s3_client()
    try:
        s3_client.head_object(Bucket=bucket_name, Key=object_key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise
