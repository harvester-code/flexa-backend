import json

from botocore.exceptions import BotoCoreError, ClientError
from loguru import logger

from app.routes.simulation.infra.sqs.client import get_sqs_client
from app.routes.simulation.infra.sqs.exceptions import (
    SQSClientInitializationError,
    SQSMessageSendError,
)


def send_message_to_sqs(queue_url: str, message_body: dict) -> None:
    """
    Send a message to an SQS queue.

    Args:
        queue_url (str): The URL of the SQS queue.
        message_body (dict): The message body to send.

    Raises:
        Exception: If sending the message fails.
    """

    # ================================================================
    try:
        sqs_client = get_sqs_client()
    except SQSClientInitializationError as _:
        logger.exception("SQS client initialization failed.")
        raise

    # ================================================================
    try:
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body),
        )
        logger.info(f"Message sent to SQS: {response['MessageId']}")

    except (BotoCoreError, ClientError) as e:
        logger.exception(f"Failed to send message to SQS. {queue_url = }")
        raise SQSMessageSendError("Failed to send message to SQS.") from e

    except Exception as e:
        logger.exception(
            f"An unexpected error occurred while sending message to SQS. {queue_url = }"
        )
        raise SQSMessageSendError(
            "An unexpected error occurred while sending message to SQS."
        ) from e
