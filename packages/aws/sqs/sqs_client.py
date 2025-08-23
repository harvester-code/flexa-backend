# Standard Library
import json
from typing import Dict, Any, List

# Third Party
import aioboto3
from botocore.exceptions import ClientError
from loguru import logger

# Application
from packages.secrets import get_secret


class SQSClient:
    """
    SQS 메시지 전송 클라이언트 - Clean Architecture Infrastructure Layer

    Lambda 시뮬레이션 요청을 위한 SQS 메시지 전송 전담 서비스
    """

    def __init__(self):
        self.region_name = get_secret("AWS_REGION")
        self.queue_url = get_secret(
            "AWS_SQS_URL"
        )  # https://sqs.ap-northeast-2.amazonaws.com/.../flexa-simulator-queue

    async def send_simulation_message(
        self, scenario_id: str, process_flow: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        """
        시뮬레이션 실행 메시지를 SQS에 전송

        Args:
            scenario_id: 시나리오 UUID
            process_flow: 공항 프로세스 단계별 설정 리스트

        Returns:
            Dict with message_id and status

        Raises:
            Exception: SQS 전송 실패 시
        """
        message_body = {"scenario_id": scenario_id, "process_flow": process_flow}

        try:
            session = aioboto3.Session(region_name=self.region_name)
            async with session.client("sqs") as sqs:
                response = await sqs.send_message(
                    QueueUrl=self.queue_url,
                    MessageBody=json.dumps(message_body, ensure_ascii=False),
                )

                message_id = response["MessageId"]
                logger.info(
                    f"🚀 SQS 메시지 전송 성공: scenario_id={scenario_id}, message_id={message_id}"
                )

                return {
                    "message_id": message_id,
                    "status": "sent",
                    "scenario_id": scenario_id,
                }

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]
            logger.error(
                f"❌ SQS 전송 실패 (AWS Error): {error_code} - {error_message}"
            )
            raise Exception(f"SQS message send failed: {error_code} - {error_message}")

        except Exception as e:
            logger.error(f"❌ SQS 전송 실패 (Unexpected Error): {str(e)}")
            raise Exception(f"Failed to send SQS message: {str(e)}")

    async def get_queue_attributes(self) -> Dict[str, Any]:
        """
        SQS 큐 상태 정보 조회 (디버깅/모니터링 용도)

        Returns:
            Queue attributes including message count, etc.
        """
        try:
            session = aioboto3.Session(region_name=self.region_name)
            async with session.client("sqs") as sqs:
                response = await sqs.get_queue_attributes(
                    QueueUrl=self.queue_url, AttributeNames=["All"]
                )

                return response.get("Attributes", {})

        except Exception as e:
            logger.error(f"❌ SQS 큐 속성 조회 실패: {str(e)}")
            raise Exception(f"Failed to get queue attributes: {str(e)}")
