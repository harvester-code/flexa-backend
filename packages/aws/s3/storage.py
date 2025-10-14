import aioboto3
import boto3
from botocore.config import Config


# 공통 boto3 세션 (awswrangler 등에서 사용)
boto3_session = boto3.Session(region_name="ap-northeast-2")


async def get_s3_client():
    config = Config(region_name="ap-northeast-2")
    session = aioboto3.Session(region_name="ap-northeast-2")
    return session.client("s3", config=config)
