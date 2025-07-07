from typing import Optional

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from app.routes.simulation.infra.sqs.exceptions import SQSClientInitializationError
from packages.secrets import get_secret

_sqs_client: Optional[boto3.client] = None


def get_sqs_client() -> boto3.client:
    """
    Get a boto3 SQS client with a specific configuration.
    If the client is already created, return the existing one.
    Otherwise, create a new client with the specified configuration.

    Returns:
        boto3.client: The SQS client.
    """
    global _sqs_client

    if _sqs_client is None:
        try:
            _sqs_client = boto3.client(
                "sqs",
                config=Config(
                    retries={
                        "max_attempts": 3,
                        "mode": "standard",
                    },
                ),
                aws_access_key_id=get_secret("AWS_ACCESS_KEY"),
                aws_secret_access_key=get_secret("AWS_SECRET_ACCESS_KEY"),
            )

        except (BotoCoreError, ClientError) as e:
            raise SQSClientInitializationError(
                "SQS 클라이언트 생성에 실패했습니다."
            ) from e

    return _sqs_client
