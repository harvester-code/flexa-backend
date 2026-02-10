# Standard Library
from datetime import datetime
from typing import List

# Third Party
from sqlalchemy import bindparam, update, and_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

# Application
from app.routes.simulation.domain.repository import ISimulationRepository
from app.routes.simulation.domain.simulation import (
    ScenarioInformation as ScenarioInformationVO,
)
from app.routes.simulation.infra.models import (
    ScenarioInformation,
    UserInformation,
)


class SimulationRepository(ISimulationRepository):
    """
    시뮬레이션 리포지토리 - Clean Architecture

    레이어 순서:
    1. 시나리오 관리 (기본 CRUD)
    2. 항공편 스케줄 처리
    3. 승객 스케줄 처리
    4. 권한 및 유틸리티
    """

    def __init__(self):
        pass

    # =====================================
    # 1. 시나리오 관리 (기본 CRUD 기능)
    # =====================================

    async def fetch_scenario_information(
        self,
        db: AsyncSession,
        user_id: str,
    ):
        """시나리오 목록 조회 (현재 사용자의 모든 시나리오)
        
        DB의 has_simulation_data 컬럼을 사용하여 시뮬레이션 데이터 존재 여부를 판단합니다.
        (S3 HEAD 요청 없이 DB 조회만으로 처리)
        """
        from loguru import logger
        
        logger.info(f"🔍 fetch_scenario_information called with user_id: {user_id}")
        
        async with db.begin():
            stmt = (
                select(
                    ScenarioInformation,
                    UserInformation.first_name,
                    UserInformation.last_name,
                    UserInformation.email,
                )
                .join(
                    UserInformation,
                    ScenarioInformation.user_id == UserInformation.user_id,
                )
                .where(
                    and_(
                        ScenarioInformation.user_id == user_id,
                        ScenarioInformation.is_active == True,
                    )
                )
                .order_by(ScenarioInformation.updated_at.desc())
                .limit(50)
            )

            result = await db.execute(stmt)
            rows = result.all()
            logger.info(f"🔍 Found {len(rows)} scenarios for user_id: {user_id}")

            scenarios = []
            for row in rows:
                scenario_info = row[0]

                scenario_dict = {
                    "id": scenario_info.id,
                    "scenario_id": scenario_info.scenario_id,
                    "user_id": str(scenario_info.user_id),
                    "editor": scenario_info.editor,
                    "name": scenario_info.name,
                    "terminal": scenario_info.terminal,
                    "airport": scenario_info.airport,
                    "memo": scenario_info.memo,
                    "target_flight_schedule_date": scenario_info.target_flight_schedule_date,
                    "is_active": scenario_info.is_active,
                    "simulation_start_at": scenario_info.simulation_start_at,
                    "created_at": scenario_info.created_at,
                    "updated_at": scenario_info.updated_at,
                    "metadata_updated_at": scenario_info.metadata_updated_at,
                    "simulation_status": scenario_info.simulation_status,
                    "simulation_end_at": scenario_info.simulation_end_at,
                    # UserInformation 필드들
                    "first_name": row[1],
                    "last_name": row[2],
                    "email": row[3],
                    # DB 컬럼에서 직접 조회 (S3 HEAD 요청 불필요)
                    "has_simulation_data": scenario_info.has_simulation_data,
                }
                scenarios.append(scenario_dict)

            return scenarios

    async def create_scenario_information(
        self,
        db: AsyncSession,
        scenario_information: ScenarioInformationVO,
    ):
        """새로운 시나리오 생성"""
        # id는 자동 생성되므로 제외하고 객체 생성
        new_scenario = ScenarioInformation(
            user_id=scenario_information.user_id,
            editor=scenario_information.editor,
            name=scenario_information.name,
            terminal=scenario_information.terminal,
            airport=scenario_information.airport,
            memo=scenario_information.memo,
            target_flight_schedule_date=scenario_information.target_flight_schedule_date,
            created_at=scenario_information.created_at,
            updated_at=scenario_information.updated_at,
            scenario_id=scenario_information.scenario_id,
        )

        db.add(new_scenario)
        await db.commit()

        return {
            "scenario_id": new_scenario.scenario_id,
            "name": new_scenario.name,
            "editor": new_scenario.editor,
            "terminal": new_scenario.terminal,
            "airport": new_scenario.airport,
            "memo": new_scenario.memo,
        }

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
        values_to_update = {}

        if name:
            values_to_update[ScenarioInformation.name] = name
        if terminal:
            values_to_update[ScenarioInformation.terminal] = terminal
        if airport:
            values_to_update[ScenarioInformation.airport] = airport
        if memo:
            values_to_update[ScenarioInformation.memo] = memo

        await db.execute(
            update(ScenarioInformation)
            .where(ScenarioInformation.scenario_id == scenario_id)
            .values(values_to_update)
        )
        await db.commit()

    async def update_metadata_updated_at(
        self,
        db: AsyncSession,
        scenario_id: str,
    ):
        """메타데이터 업데이트 시각 갱신"""
        from datetime import datetime, timezone
        
        await db.execute(
            update(ScenarioInformation)
            .where(ScenarioInformation.scenario_id == scenario_id)
            .values(metadata_updated_at=datetime.now(timezone.utc).replace(microsecond=0))
        )
        await db.commit()

    async def update_simulation_start_at(
        self,
        db: AsyncSession,
        scenario_id: str,
    ):
        """시뮬레이션 시작 시각 및 상태 갱신"""
        from datetime import datetime
        from loguru import logger
        
        try:
            from datetime import timezone
            current_time = datetime.now(timezone.utc).replace(microsecond=0)
            logger.info(f"🕐 Setting simulation_start_at to: {current_time} for scenario: {scenario_id}")
            
            result = await db.execute(
                update(ScenarioInformation)
                .where(ScenarioInformation.scenario_id == scenario_id)
                .values(
                    simulation_start_at=current_time,
                    simulation_status="processing",  # 🔴 즉시 processing 상태로 변경
                    simulation_error=None,  # 🔴 이전 에러 메시지 리셋
                    simulation_end_at=None,  # 🔴 이전 종료 시각 리셋
                    has_simulation_data=False,  # 🔴 재실행 시 시뮬레이션 데이터 플래그 리셋
                )
            )
            
            rows_affected = result.rowcount
            logger.info(f"📝 Update query executed, rows affected: {rows_affected}")
            logger.info(f"🚀 simulation_status set to 'processing' for scenario: {scenario_id}")
            logger.info(f"🧹 Previous error and end_at cleared for fresh start")
            
            if rows_affected == 0:
                logger.warning(f"⚠️ No rows updated for scenario_id: {scenario_id}")
            
            await db.commit()
            logger.info(f"✅ Transaction committed for scenario: {scenario_id}")
            
        except Exception as e:
            logger.error(f"❌ Error in update_simulation_start_at: {str(e)}")
            await db.rollback()
            logger.error(f"🔄 Transaction rolled back for scenario: {scenario_id}")
            raise

    async def deactivate_scenario_information(self, db: AsyncSession, ids: List[str]):
        """시나리오 소프트 삭제"""
        stmt = (
            update(ScenarioInformation)
            .where(
                ScenarioInformation.scenario_id.in_(bindparam("ids", expanding=True))
            )
            .values(is_active=False)
        )
        await db.execute(stmt, {"ids": ids})
        await db.commit()

    async def delete_scenarios_permanently(self, db: AsyncSession, ids: List[str]):
        """시나리오 영구 삭제 (하드 삭제)"""
        from sqlalchemy import delete

        stmt = delete(ScenarioInformation).where(
            ScenarioInformation.scenario_id.in_(bindparam("ids", expanding=True))
        )
        await db.execute(stmt, {"ids": ids})
        await db.commit()

    # =====================================
    # 2. 항공편 스케줄 처리 (Flight Schedule)
    # =====================================

    async def get_scenario_by_id(
        self,
        db: AsyncSession,
        scenario_id: str,
    ):
        """시나리오 ID로 단일 시나리오 조회"""
        stmt = (
            select(ScenarioInformation)
            .where(
                and_(
                    ScenarioInformation.scenario_id == scenario_id,
                    ScenarioInformation.is_active == True,
                )
            )
        )

        result = await db.execute(stmt)
        scenario = result.scalar_one_or_none()

        return scenario

    async def get_scenarios_by_name_pattern(
        self,
        db: AsyncSession,
        user_id: str,
        base_name: str,
    ):
        """특정 베이스 이름으로 시작하는 시나리오들 조회

        베이스 이름과 정확히 일치하거나,
        베이스 이름 + " (숫자)" 패턴을 가진 시나리오들을 조회합니다.
        """
        stmt = (
            select(ScenarioInformation)
            .where(
                and_(
                    ScenarioInformation.user_id == user_id,
                    ScenarioInformation.is_active == True,
                    # 베이스 이름으로 시작하는 모든 시나리오
                    ScenarioInformation.name.like(f"{base_name}%")
                )
            )
        )

        result = await db.execute(stmt)
        scenarios = result.scalars().all()

        # 정확한 베이스 이름이거나 "(숫자)" 패턴을 가진 것만 필터링
        import re
        pattern = re.compile(rf'^{re.escape(base_name)}(?:\s*\(\d+\))?$')
        filtered_scenarios = [s for s in scenarios if pattern.match(s.name)]

        return filtered_scenarios

    async def update_scenario_target_flight_schedule_date(
        self,
        db: AsyncSession,
        scenario_id: str,
        target_flight_schedule_date,
    ):
        """시나리오 대상 항공편 스케줄 날짜 업데이트"""
        # 문자열 날짜를 datetime 객체로 변환 후 다시 문자열로 변환 (검증 목적)
        if isinstance(target_flight_schedule_date, str):
            # 날짜 형식 검증을 위해 datetime으로 파싱 후 다시 문자열로 변환
            target_date_obj = datetime.strptime(target_flight_schedule_date, "%Y-%m-%d")
            target_date_str = target_date_obj.strftime("%Y-%m-%d")
        else:
            # datetime 객체인 경우 문자열로 변환
            target_date_str = target_flight_schedule_date.strftime("%Y-%m-%d")

        await db.execute(
            update(ScenarioInformation)
            .where(ScenarioInformation.scenario_id == scenario_id)
            .values({ScenarioInformation.target_flight_schedule_date: target_date_str})
        )
        await db.commit()

    # =====================================
    # 3. 승객 스케줄 처리 (Show-up Passenger)
    # =====================================

    # =====================================
    # 4. 시뮬레이션 상태 및 권한 관리
    # =====================================


    async def check_user_scenario_permission(
        self, db: AsyncSession, user_id: str, scenario_id: str
    ):
        """사용자 시나리오 권한 확인"""
        result = await db.execute(
            select(1)
            .where(ScenarioInformation.scenario_id == scenario_id)
            .where(ScenarioInformation.user_id == user_id)
        )

        exists = result.scalar() is not None
        if not exists:
            raise ValueError("User does not have permission for this scenario")

    async def check_scenario_exists(
        self, db: AsyncSession, scenario_id: str, user_id: str | None = None
    ) -> bool:
        """시나리오 존재 여부 확인 (권한 검증 포함)"""
        if not scenario_id or not scenario_id.strip():
            return False

        try:
            if user_id:
                # 사용자 권한까지 확인 (해당 사용자의 시나리오인지 확인) - ORM 사용
                stmt = (
                    select(func.count())
                    .select_from(ScenarioInformation)
                    .where(
                        and_(
                            ScenarioInformation.scenario_id == scenario_id,
                            ScenarioInformation.user_id == user_id,
                            ScenarioInformation.is_active == True,
                        )
                    )
                )
                result = await db.execute(stmt)
                count = result.scalar_one_or_none()
                return count > 0 if count else False
            else:
                # 시나리오 존재 여부만 확인
                result = await db.execute(
                    select(ScenarioInformation.scenario_id).where(
                        and_(
                            ScenarioInformation.scenario_id == scenario_id,
                            ScenarioInformation.is_active == True,
                        )
                    )
                )
                return result.scalar_one_or_none() is not None
        except Exception:
            return False
