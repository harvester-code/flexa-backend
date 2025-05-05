from typing import Optional

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from src.simulation.infra.sqs.exceptions import SQSClientInitializationError

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
            )

        except (BotoCoreError, ClientError) as e:
            raise SQSClientInitializationError(
                "SQS 클라이언트 생성에 실패했습니다."
            ) from e

    return _sqs_client
