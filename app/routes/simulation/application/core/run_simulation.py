"""
시뮬레이션 실행 처리 통합 모듈 (Run Simulation Processing)

이 모듈은 시뮬레이션 실행 처리의 Storage와 Response 기능을 통합합니다:
- RunSimulationStorage: SQS 메시지 전송을 통한 Lambda 시뮬레이션 트리거
- RunSimulationResponse: 프론트엔드용 JSON 응답 생성 (실행 상태 포함)
"""

from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import HTTPException, status
from loguru import logger

from packages.aws.sqs.sqs_client import SQSClient


class RunSimulationStorage:
    """시뮬레이션 실행 저장 전담 클래스"""

    def __init__(self):
        self._sqs_client = None  # Lazy initialization

    async def execute_simulation(
        self, scenario_id: str, process_flow: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        """
        시뮬레이션 실행 요청 - SQS 메시지 전송

        Args:
            scenario_id: 시나리오 UUID
            process_flow: 공항 프로세스 단계별 설정 리스트

        Returns:
            Dict with message_id, status, scenario_id

        Raises:
            Exception: SQS 전송 실패 시
        """
        try:
            # Lazy initialization of SQS client
            if self._sqs_client is None:
                self._sqs_client = SQSClient()

            # SQS로 메시지 전송
            result = await self._sqs_client.send_simulation_message(
                scenario_id=scenario_id,
                process_flow=process_flow,
            )

            logger.info(f"🚀 시뮬레이션 실행 요청 완료: scenario_id={scenario_id}")
            return result

        except Exception as e:
            logger.error(f"SQS message sending failed: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to queue simulation: {str(e)}",
            )

    async def save_simulation_result(self, scenario_id: str, result_data: dict):
        """
        시뮬레이션 결과를 S3에 저장 (향후 구현)

        Args:
            scenario_id: 시나리오 ID
            result_data: 시뮬레이션 결과 데이터
        """
        # TODO: 시뮬레이션 결과를 S3에 저장하는 로직 구현
        # 현재는 Lambda에서 직접 S3에 저장하므로 추후 필요시 구현
        pass


class RunSimulationResponse:
    """시뮬레이션 결과 프론트엔드 응답 생성 전담 클래스"""

    async def build_response(
        self, scenario_id: str, simulation_result: Optional[pd.DataFrame] = None
    ) -> Dict:
        """
        시뮬레이션 결과 응답 데이터 구성

        Args:
            scenario_id: 시나리오 ID
            simulation_result: 시뮬레이션 결과 DataFrame (선택적)

        Returns:
            프론트엔드용 응답 딕셔너리
        """

        # 현재는 SQS 전송 결과만 반환
        # 향후 시뮬레이션 완료 후 결과 데이터 추가 예정
        base_response = {
            "scenario_id": scenario_id,
            "status": "queued",
            "message": "Simulation has been queued for execution",
        }

        # 시뮬레이션 결과가 있는 경우 추가 데이터 구성
        if simulation_result is not None:
            base_response.update(
                await self._build_simulation_analysis(simulation_result)
            )

        return base_response

    async def _build_simulation_analysis(self, simulation_df: pd.DataFrame) -> Dict:
        """
        시뮬레이션 결과 분석 데이터 구성 (향후 구현)

        Args:
            simulation_df: 시뮬레이션 결과 DataFrame

        Returns:
            분석 결과 딕셔너리
        """
        # TODO: 시뮬레이션 결과 분석 로직 구현
        # - 대기열 길이 분석
        # - 처리 시간 분석
        # - 병목 지점 분석
        # - 차트 데이터 생성

        return {
            "analysis": {
                "total_passengers": (
                    len(simulation_df) if not simulation_df.empty else 0
                ),
                "avg_waiting_time": 0,  # 실제 계산 로직 필요
                "max_queue_length": 0,  # 실제 계산 로직 필요
                "bottlenecks": [],  # 병목 지점 분석 결과
            },
            "charts": {
                "queue_length_over_time": [],  # 시간별 대기열 길이
                "processing_time_distribution": [],  # 처리 시간 분포
                "facility_utilization": [],  # 시설 이용률
            },
            "status": "completed",
        }
