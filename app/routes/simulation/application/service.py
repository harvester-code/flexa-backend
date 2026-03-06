"""
시뮬레이션 서비스 - 정리된 버전

이 서비스는 다음 기능들을 제공합니다:
1. 시나리오 관리 (생성, 조회)
2. 항공편 스케줄 처리 (FlightScheduleGenerator 사용)
3. 승객 스케줄 처리 (PassengerGenerator 사용)
4. 시뮬레이션 실행 (SQS 메시지)
5. 메타데이터 처리 (S3 저장/로드)
"""

from datetime import datetime, timezone
from typing import Any, Dict, List

from fastapi import HTTPException, status
from loguru import logger
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncSession
from ulid import ULID

# Application - Core Structure
from app.routes.simulation.application.core import (
    FlightScheduleStorage,
    FlightScheduleResponse,
    FlightFiltersResponse,
    ShowUpPassengerStorage,
    ShowUpPassengerResponse,
    RunSimulationStorage,
    RunSimulationResponse,
)
from app.routes.simulation.domain.simulation import (
    ScenarioInformation,
)

# Packages
from packages.aws.s3.s3_manager import S3Manager


class SimulationService:
    def __init__(self, simulation_repo, s3_manager: S3Manager):
        self.simulation_repo = simulation_repo
        self.s3_manager = s3_manager

        # Storage layer instances (DI 싱글톤 S3Manager 전달)
        self.flight_storage = FlightScheduleStorage(s3_manager=s3_manager)
        self.passenger_storage = ShowUpPassengerStorage(s3_manager=s3_manager)
        self.simulation_storage = RunSimulationStorage()

        # Response layer instances
        self.flight_response = FlightScheduleResponse()
        self.flight_filters_response = FlightFiltersResponse()
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
                created_at=datetime.now(timezone.utc).replace(microsecond=0),
                updated_at=datetime.now(timezone.utc).replace(microsecond=0),
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

    async def delete_scenarios(
        self, db: AsyncSession, scenario_ids: List[str], user_id: str
    ):
        """권한 검증을 포함한 시나리오 소프트 삭제 (is_active=False) + S3 데이터 삭제
        
        Supabase에서 is_active 플래그를 False로 변경하고, S3 시나리오 폴더도 삭제합니다.
        S3 버저닝이 활성화되어 있으므로 삭제 후에도 90일간 noncurrent version으로 복구 가능합니다.
        90일 후 S3 lifecycle 정책과 Supabase 크론잡에 의해 양쪽 모두 완전 삭제됩니다.
        """
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

            # 💾 Supabase에서 소프트 삭제 (is_active=False)
            await self.simulation_repo.deactivate_scenario_information(db, scenario_ids)
            logger.info(f"✅ Soft deleted {len(scenario_ids)} scenarios (is_active=False)")

            # 🗑️ S3 데이터 삭제 (버저닝으로 90일간 복구 가능)
            for scenario_id in scenario_ids:
                success = await self.s3_manager.delete_scenario_data(scenario_id)
                if not success:
                    logger.warning(f"⚠️ S3 deletion failed for {scenario_id}, but soft delete succeeded")

            return {"message": f"Successfully deleted {len(scenario_ids)} scenarios"}

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to soft delete scenarios {scenario_ids}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to delete scenarios",
            )

    async def copy_scenario_information(
        self, db: AsyncSession, source_scenario_id: str, user_id: str, new_name: str = None
    ):
        """
        시나리오 복사 - Supabase 데이터와 S3 데이터 모두 복사

        1. 원본 시나리오 조회
        2. 새 시나리오 생성 (새 UUID)
        3. S3 데이터 복사
        4. 새 시나리오 정보 반환

        Args:
            new_name: 프론트엔드에서 전달한 새 시나리오 이름 (선택사항)
        """
        try:
            # 1. 원본 시나리오 조회
            source_scenario = await self.simulation_repo.get_scenario_by_id(
                db, source_scenario_id
            )

            if not source_scenario:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Source scenario '{source_scenario_id}' not found",
                )

            # 2. 새 시나리오 ID 생성
            new_scenario_id = str(ULID())

            # 3. 복사된 시나리오 이름 생성
            if new_name:
                # 프론트엔드에서 이름을 전달한 경우 그대로 사용
                final_name = new_name
            else:
                # 이름이 없는 경우 번호 자동 증가
                import re

                # 기존 이름에서 (숫자) 패턴 제거하여 베이스 이름 추출
                # 예: "시나리오A (3)" → "시나리오A"
                base_name = re.sub(r'\s*\(\d+\)\s*$', '', source_scenario.name).strip()

                # 같은 베이스 이름을 가진 시나리오들 조회
                similar_scenarios = await self.simulation_repo.get_scenarios_by_name_pattern(
                    db, user_id, base_name
                )

                # 가장 큰 번호 찾기
                max_number = 0
                pattern = re.compile(rf'^{re.escape(base_name)}\s*\((\d+)\)\s*$')

                for scenario in similar_scenarios:
                    match = pattern.match(scenario.name)
                    if match:
                        number = int(match.group(1))
                        max_number = max(max_number, number)

                # 다음 번호로 이름 생성
                final_name = f"{base_name} ({max_number + 1})"

            # 4. 새 시나리오 정보 생성
            new_scenario_info = ScenarioInformation(
                id=None,
                user_id=user_id,
                editor=source_scenario.editor,
                name=final_name,
                terminal=source_scenario.terminal,
                airport=source_scenario.airport,
                memo=source_scenario.memo,
                target_flight_schedule_date=source_scenario.target_flight_schedule_date,
                created_at=datetime.now(timezone.utc).replace(microsecond=0),
                updated_at=datetime.now(timezone.utc).replace(microsecond=0),
                scenario_id=new_scenario_id,
            )

            # 4. DB에 새 시나리오 저장
            created_scenario = await self.simulation_repo.create_scenario_information(
                db, new_scenario_info
            )

            # 5. S3 데이터 복사 (비동기 처리)
            try:
                await self.s3_manager.copy_scenario_data(
                    source_scenario_id=source_scenario_id,
                    target_scenario_id=new_scenario_id,
                )
                logger.info(f"✅ S3 data copied: {source_scenario_id} → {new_scenario_id}")
            except Exception as s3_error:
                # S3 복사 실패는 경고만 기록 (시나리오는 이미 생성됨)
                logger.warning(f"⚠️ S3 data copy failed (scenario created): {str(s3_error)}")

            # 6. 생성된 시나리오 정보 반환
            return {
                "scenario_id": new_scenario_id,
                "name": new_scenario_info.name,
                "terminal": new_scenario_info.terminal,
                "airport": new_scenario_info.airport,
                "memo": new_scenario_info.memo,
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to copy scenario {source_scenario_id}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to copy scenario",
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

            # 2. 응답 생성 (Response Layer) - 컨텍스트 정보 포함
            return await self.flight_response.build_response(
                flight_data, 
                conditions, 
                flight_type,
                airport=airport,
                date=date,
                scenario_id=scenario_id,
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

            # 2. 응답 생성 (Response Layer) - 컨텍스트 정보 포함
            settings = config.get("settings", {})
            return await self.passenger_response.build_response(
                passenger_data, 
                config,
                airport=settings.get("airport"),
                date=settings.get("date"),
                scenario_id=scenario_id,
            )
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
        self, scenario_id: str, setting: Dict[str, Any], process_flow: List[Dict[str, Any]], db=None
    ) -> Dict[str, str]:
        """
        시뮬레이션 실행 요청 - SQS 메시지 전송

        Args:
            scenario_id: 시나리오 UUID
            setting: 시뮬레이션 기본 설정 (airport, date, scenario_id)
            process_flow: 공항 프로세스 단계별 설정 리스트
            db: 데이터베이스 세션 (선택적)

        Returns:
            Dict with message_id, status, scenario_id

        Raises:
            Exception: SQS 전송 실패 시
        """
        try:
            logger.info(f"🎯 Starting run_simulation for scenario: {scenario_id}")
            
            # 0. 시뮬레이션 시작 시간 업데이트 (SQS 전송 전)
            if db is not None:
                try:
                    logger.info(f"🔄 Attempting to update simulation_start_at for scenario: {scenario_id}")
                    await self.simulation_repo.update_simulation_start_at(db, scenario_id)
                    logger.info(f"✅ Successfully updated simulation_start_at for scenario {scenario_id}")
                except Exception as db_error:
                    logger.error(f"❌ Failed to update simulation_start_at for scenario {scenario_id}: {str(db_error)}")
                    logger.error(f"❌ Exception type: {type(db_error)}")
                    import traceback
                    logger.error(f"❌ Traceback: {traceback.format_exc()}")
                    # DB 업데이트 실패해도 시뮬레이션은 계속 진행
            else:
                logger.warning(f"⚠️ Database session is None for scenario {scenario_id}")

            # 1. 시뮬레이션 실행 (Storage Layer)
            storage_result = await self.simulation_storage.execute_simulation(
                scenario_id=scenario_id,
                setting=setting,
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

    async def save_scenario_metadata(self, scenario_id: str, metadata: dict, db=None):
        """
        시나리오 메타데이터를 S3에 저장하고 Supabase의 metadata_updated_at도 업데이트

        메타데이터 구조:
        - tabs: 각 탭별 백엔드 body 데이터
        - simulationUI: UI 전용 상태 데이터 (parquetMetadata 등)
        """
        try:
            from datetime import datetime

            # 메타데이터 구조 로깅 (디버깅용)
            tabs_count = len(metadata.get("tabs", {}))
            has_simulation_ui = "simulationUI" in metadata

            logger.info(
                f"💾 Saving metadata for scenario {scenario_id}: "
                f"{tabs_count} tabs, simulationUI: {has_simulation_ui}"
            )

            # S3Manager를 사용하여 저장
            success = await self.s3_manager.save_json_async(
                scenario_id=scenario_id,
                filename="metadata-for-frontend.json",
                data=metadata
            )

            if success:
                # S3 저장 성공 후 Supabase의 metadata_updated_at도 업데이트
                if db is not None:
                    try:
                        await self.simulation_repo.update_metadata_updated_at(db, scenario_id)
                        logger.info(
                            f"Updated metadata_updated_at in Supabase for scenario {scenario_id}"
                        )
                    except Exception as db_error:
                        logger.warning(
                            f"Failed to update metadata_updated_at in Supabase: {str(db_error)}"
                        )
                        # DB 업데이트 실패해도 S3 저장은 성공했으므로 계속 진행

                logger.info(
                    f"Successfully saved metadata to S3 for scenario {scenario_id}"
                )
                return {
                    "message": "Metadata saved successfully",
                    "scenario_id": scenario_id,
                    "saved_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                }
            else:
                raise Exception("Failed to save metadata to S3")

        except Exception as e:
            logger.error(f"Failed to save metadata to S3: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to save metadata to S3: {str(e)}",
            )

    async def load_scenario_metadata(self, scenario_id: str):
        """S3에서 시나리오 메타데이터 로드"""
        try:
            from datetime import datetime

            # S3Manager를 사용하여 로드
            metadata = await self.s3_manager.get_json_async(
                scenario_id=scenario_id,
                filename="metadata-for-frontend.json"
            )

            if metadata is not None:
                logger.info(
                    f"Successfully loaded metadata from S3 for scenario {scenario_id}"
                )
                return {
                    "scenario_id": scenario_id,
                    "metadata": metadata,
                    "loaded_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                }
            else:
                # 파일이 없는 경우 - 빈 메타데이터 반환 (정상적인 상황)
                logger.info(
                    f"No metadata file found for scenario {scenario_id} - returning empty metadata"
                )
                return {
                    "scenario_id": scenario_id,
                    "metadata": None,
                    "loaded_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                    "is_new_scenario": True
                }

        except Exception as e:
            # 실제 AWS 연결 문제나 권한 문제 등만 500 에러로 처리
            logger.error(f"Failed to load metadata from S3: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to load metadata from S3: {str(e)}",
            )

    async def delete_scenario_metadata(self, scenario_id: str):
        """
        S3에서 시나리오 메타데이터 삭제

        모든 메타데이터를 삭제합니다:
        - tabs: 각 탭별 백엔드 body 데이터
        - simulationUI: UI 전용 상태 데이터
        """
        try:
            from datetime import datetime

            # S3Manager를 사용하여 삭제
            success = await self.s3_manager.delete_json_async(
                scenario_id=scenario_id,
                filename="metadata-for-frontend.json"
            )

            if success:
                logger.info(
                    f"Successfully deleted metadata from S3 for scenario {scenario_id}"
                )
            else:
                logger.info(
                    f"Metadata file for scenario {scenario_id} was already deleted or does not exist"
                )

            return {
                "message": "Metadata deleted successfully" if success else "Metadata was already deleted or does not exist",
                "scenario_id": scenario_id,
                "deleted_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            }

        except Exception as e:
            logger.error(f"Failed to delete metadata from S3: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete metadata from S3: {str(e)}",
            )

    async def get_flight_filters_metadata(
        self, snowflake_db: Connection, scenario_id: str, airport: str, date: str
    ) -> dict:
        """
        항공편 필터링 메타데이터 생성 (실제 데이터 기반)

        Departure/Arrival 모드별 필터 옵션을 제공합니다.
        실제 Snowflake 데이터에서 메타정보를 추출합니다.
        """
        return await self.flight_filters_response.generate_filters_metadata(
            snowflake_db=snowflake_db, scenario_id=scenario_id, airport=airport, date=date
        )
