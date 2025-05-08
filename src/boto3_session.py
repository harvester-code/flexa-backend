# 임시용
import os

import boto3

boto3_session = boto3.Session(
    region_name="ap-northeast-2",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)
