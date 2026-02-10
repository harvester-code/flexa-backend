# Standard Library
import json
from typing import Dict, Any, List

# Third Party
import aioboto3
from botocore.exceptions import ClientError
from loguru import logger

# Application
from packages.doppler.client import get_secret

# 싱글톤 aioboto3 세션 (SQS용 - 애플리케이션 전체에서 재사용)
_sqs_session = None


def _get_sqs_session() -> aioboto3.Session:
    """SQS용 싱글톤 aioboto3 세션 반환"""
    global _sqs_session
    if _sqs_session is None:
        region = get_secret("AWS_REGION")
        _sqs_session = aioboto3.Session(region_name=region)
        logger.info(f"[SQS] Created singleton aioboto3 session (region={region})")
    return _sqs_session


class SQSClient:
    """
    SQS 메시지 전송 클라이언트 - Clean Architecture Infrastructure Layer

    Lambda 시뮬레이션 요청을 위한 SQS 메시지 전송 전담 서비스
    """

    def __init__(self):
        self.queue_url = get_secret("AWS_SQS_URL")

    async def send_simulation_message(
        self, scenario_id: str, setting: Dict[str, Any], process_flow: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        """
        시뮬레이션 실행 메시지를 SQS에 전송

        Args:
            scenario_id: 시나리오 UUID
            setting: 시뮬레이션 기본 설정 (airport, date, scenario_id)
            process_flow: 공항 프로세스 단계별 설정 리스트

        Returns:
            Dict with message_id and status

        Raises:
            Exception: SQS 전송 실패 시
        """
        message_body = {
            "scenario_id": scenario_id, 
            "setting": setting,
            "process_flow": process_flow
        }

        try:
            session = _get_sqs_session()
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
