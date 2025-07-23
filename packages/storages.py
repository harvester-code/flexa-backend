import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError
from loguru import logger


async def get_s3_client():
    config = Config(region_name="ap-northeast-2")
    session = aioboto3.Session()
    return session.client("s3", config=config)


async def check_s3_object_exists(bucket_name: str, object_key: str) -> bool:
    async with await get_s3_client() as s3_client:
        try:
            await s3_client.head_object(Bucket=bucket_name, Key=object_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.info(f"Object not found: Bucket={bucket_name}, Key={object_key}")
                return False
            logger.error(f"ClientError occurred: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
            raise
