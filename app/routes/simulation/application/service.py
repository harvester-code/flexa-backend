"""
시뮬레이션 서비스 - 정리된 버전

이 서비스는 다음 기능들을 제공합니다:
1. 시나리오 관리 (생성, 조회)
2. 항공편 스케줄 처리 (FlightScheduleGenerator 사용)
3. 승객 스케줄 처리 (PassengerGenerator 사용)
4. 시뮬레이션 실행 (SQS 메시지)
5. 메타데이터 처리 (S3 저장/로드)
"""

from datetime import datetime
from typing import Any, Dict, List

import awswrangler as wr
from fastapi import HTTPException, status
from loguru import logger
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from ulid import ULID

# Application - Core Structure
from app.routes.simulation.application.core import (
    FlightScheduleStorage,
    FlightScheduleResponse,
    ShowUpPassengerStorage,
    ShowUpPassengerResponse,
    RunSimulationStorage,
    RunSimulationResponse,
)
from app.routes.simulation.domain.simulation import (
    ScenarioInformation,
)
from app.routes.simulation.infra.models import UserInformation

# Packages
from packages.doppler.client import get_secret
from packages.aws.s3.storage import boto3_session


class SimulationService:
    def __init__(self, simulation_repo):
        self.simulation_repo = simulation_repo

        # Storage layer instances
        self.flight_storage = FlightScheduleStorage()
        self.passenger_storage = ShowUpPassengerStorage()
        self.simulation_storage = RunSimulationStorage()

        # Response layer instances
        self.flight_response = FlightScheduleResponse()
        self.passenger_response = ShowUpPassengerResponse()
        self.simulation_response = RunSimulationResponse()

    # =====================================
    # 1. 시나리오 관리 (Scenario Management)
    # =====================================

    async def fetch_scenario_information(self, db: AsyncSession, user_id: str):
        """시나리오 목록 조회 (마스터/사용자 시나리오 구분)"""
        try:
            return await self.simulation_repo.fetch_scenario_information(db, user_id)
        except Exception as e:
            logger.error(f"Failed to fetch scenarios for user {user_id}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to fetch scenario information",
            )

    async def create_scenario_information(
        self,
        db: AsyncSession,
        user_id: str,
        name: str,
        editor: str,
        terminal: str,
        airport: str | None,
        memo: str | None,
    ):
        """새로운 시나리오 생성"""
        try:
            # 시나리오 ID 생성
            scenario_id = str(ULID())

            # ScenarioInformation 생성
            scenario_info = ScenarioInformation(
                id=None,
                user_id=user_id,
                editor=editor,
                name=name,
                terminal=terminal,
                airport=airport,
                memo=memo,
                target_flight_schedule_date=None,
                created_at=datetime.now(),
                updated_at=datetime.now(),
                scenario_id=scenario_id,
            )

            return await self.simulation_repo.create_scenario_information(
                db, scenario_info
            )
        except Exception as e:
            logger.error(f"Failed to create scenario: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create scenario",
            )

    async def update_scenario_information(
        self,
        db: AsyncSession,
        scenario_id: str,
        name: str | None,
        terminal: str | None,
        airport: str | None,
        memo: str | None,
    ):
        """시나리오 정보 수정"""
        try:
            return await self.simulation_repo.update_scenario_information(
                db, scenario_id, name, terminal, airport, memo
            )
        except Exception as e:
            logger.error(f"Failed to update scenario {scenario_id}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update scenario information",
            )

    async def deactivate_scenario_information(self, db: AsyncSession, ids: List[str]):
        """시나리오 소프트 삭제"""
        try:
            return await self.simulation_repo.deactivate_scenario_information(db, ids)
        except Exception as e:
            logger.error(f"Failed to deactivate scenarios {ids}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to deactivate scenarios",
            )

    async def deactivate_scenario_information_with_validation(
        self, db: AsyncSession, scenario_ids: List[str], user_id: str
    ):
        """권한 검증을 포함한 시나리오 bulk 소프트 삭제"""
        try:
            # 🔒 각 시나리오에 대한 권한 검증
            for scenario_id in scenario_ids:
                scenario_exists = await self.validate_scenario_exists(
                    db, scenario_id, user_id
                )
                if not scenario_exists:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Scenario '{scenario_id}' not found or you don't have permission to access it.",
                    )

            # ✅ 권한 검증 완료, bulk 삭제 실행
            return await self.simulation_repo.deactivate_scenario_information(
                db, scenario_ids
            )

        except HTTPException:
            # HTTPException은 그대로 재발생
            raise
        except Exception as e:
            logger.error(
                f"Failed to deactivate scenarios with validation {scenario_ids}: {str(e)}"
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to deactivate scenarios",
            )

    async def update_scenario_target_flight_schedule_date(
        self, db: AsyncSession, scenario_id: str, date: str
    ):
        """시나리오 대상 항공편 스케줄 날짜 업데이트"""
        try:
            return (
                await self.simulation_repo.update_scenario_target_flight_schedule_date(
                    db, scenario_id, date
                )
            )
        except Exception as e:
            logger.error(
                f"Failed to update target date for scenario {scenario_id}: {str(e)}"
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update scenario target date",
            )

    async def get_user_information_by_id(
        self, db: AsyncSession, user_id: str
    ) -> UserInformation | None:
        """사용자 정보 조회"""
        try:
            query = select(UserInformation).where(
                UserInformation.supabase_user_id == user_id
            )
            result = await db.execute(query)
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Failed to get user information for {user_id}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to get user information",
            )

    # =====================================
    # 2. 항공편 스케줄 처리 (Flight Schedule)
    # =====================================

    async def generate_scenario_flight_schedule(
        self,
        db: Connection,
        date: str,
        airport: str,
        flight_type: str,
        conditions: list | None,
        scenario_id: str,
    ):
        """시나리오별 항공편 스케줄 조회 및 차트 데이터 생성"""
        try:
            # 1. 데이터 저장 (Storage Layer)
            flight_data = await self.flight_storage.fetch_and_store(
                db,
                date,
                airport,
                flight_type,
                conditions,
                scenario_id,
                storage="redshift",
            )

            # 2. 응답 생성 (Response Layer)
            return await self.flight_response.build_response(
                flight_data, conditions, flight_type
            )
        except Exception as e:
            logger.error(
                f"Flight schedule generation failed for scenario {scenario_id}: {str(e)}"
            )
            raise  # Storage에서 이미 HTTPException을 발생시키므로 재발생

    # =====================================
    # 3. 승객 스케줄 처리 (Show-up Passenger)
    # =====================================

    async def generate_passenger_schedule(
        self,
        scenario_id: str,
        config: dict,
    ):
        """승객 스케줄 생성 - pax_simple.json 구조 기반"""
        try:
            # 1. 데이터 저장 (Storage Layer)
            passenger_data = await self.passenger_storage.generate_and_store(
                scenario_id, config
            )

            # 2. 응답 생성 (Response Layer)
            return await self.passenger_response.build_response(passenger_data, config)
        except Exception as e:
            logger.error(
                f"Passenger schedule generation failed for scenario {scenario_id}: {str(e)}"
            )
            raise  # Storage에서 이미 HTTPException을 발생시키므로 재발생

    async def validate_scenario_exists(
        self, db: AsyncSession, scenario_id: str, user_id: str | None = None
    ) -> bool:
        """시나리오 존재 여부 검증"""
        try:
            return await self.simulation_repo.check_scenario_exists(
                db, scenario_id, user_id
            )
        except Exception as e:
            logger.error(
                f"Failed to validate scenario {scenario_id} existence: {str(e)}"
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to validate scenario existence",
            )

    # =====================================
    # 4. 시뮬레이션 실행 (Run Simulation)
    # =====================================

    async def run_simulation(
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
            # 1. 시뮬레이션 실행 (Storage Layer)
            storage_result = await self.simulation_storage.execute_simulation(
                scenario_id=scenario_id,
                process_flow=process_flow,
            )

            # 2. 응답 생성 (Response Layer)
            response_result = await self.simulation_response.build_response(
                scenario_id=scenario_id
            )

            # Storage와 Response 결과 병합
            return {**storage_result, **response_result}

        except Exception as e:
            logger.error(f"Simulation execution failed: {str(e)}")
            raise  # Storage에서 이미 HTTPException을 발생시키므로 재발생

    # =====================================
    # 5. 메타데이터 처리 (S3 Save/Load)
    # =====================================

    async def save_scenario_metadata(self, scenario_id: str, metadata: dict):
        """시나리오 메타데이터를 S3에 저장"""
        try:
            import json
            from datetime import datetime

            # JSON 문자열로 변환
            json_content = json.dumps(metadata, ensure_ascii=False, indent=2)

            # boto3를 사용하여 직접 S3에 업로드
            s3_client = boto3_session.client("s3")
            bucket_name = get_secret("AWS_S3_BUCKET_NAME")
            s3_key = f"{scenario_id}/metadata-for-frontend.json"

            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=json_content,
                ContentType="application/json",
                ContentEncoding="utf-8",
            )

            logger.info(
                f"Successfully saved metadata to S3: s3://{bucket_name}/{s3_key}"
            )

            return {
                "message": "Metadata saved successfully",
                "scenario_id": scenario_id,
                "s3_key": s3_key,
                "bucket": bucket_name,
                "saved_at": datetime.now().isoformat(),
            }
        except Exception as e:
            logger.error(f"Failed to save metadata to S3: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to save metadata to S3: {str(e)}",
            )

    async def load_scenario_metadata(self, scenario_id: str):
        """S3에서 시나리오 메타데이터 로드"""
        try:
            import json
            from datetime import datetime

            bucket_name = get_secret("AWS_S3_BUCKET_NAME")
            s3_key = f"{scenario_id}/metadata-for-frontend.json"
            s3_client = boto3_session.client("s3")

            try:
                # S3에서 객체 가져오기
                response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
                json_content = response["Body"].read().decode("utf-8")
                metadata = json.loads(json_content)

                logger.info(
                    f"Successfully loaded metadata from S3: s3://{bucket_name}/{s3_key}"
                )

                return {
                    "scenario_id": scenario_id,
                    "metadata": metadata,
                    "s3_key": s3_key,
                    "loaded_at": datetime.now().isoformat(),
                }

            except s3_client.exceptions.NoSuchKey:
                # 파일이 없는 경우 - 새 시나리오이므로 빈 메타데이터 반환
                logger.info(
                    f"No metadata file found for scenario {scenario_id} - returning empty metadata"
                )
                return {
                    "scenario_id": scenario_id,
                    "metadata": {"tabs": {}},
                    "s3_key": s3_key,
                    "loaded_at": datetime.now().isoformat(),
                }

        except Exception as e:
            # 실제 AWS 연결 문제나 권한 문제 등만 500 에러로 처리
            logger.error(f"Failed to load metadata from S3: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to load metadata from S3: {str(e)}",
            )
