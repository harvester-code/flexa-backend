# 임시용
import boto3

from packages.secrets import get_secret

boto3_session = boto3.Session(
    region_name="ap-northeast-2",
    # aws_access_key_id=get_secret("AWS_ACCESS_KEY"),
    # aws_secret_access_key=get_secret("AWS_SECRET_ACCESS_KEY"),
)
