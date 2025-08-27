# Standard Library
from datetime import datetime
from typing import List

# Third Party
from sqlalchemy import bindparam, text, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

# Application
from app.routes.simulation.domain.repository import ISimulationRepository
from app.routes.simulation.domain.simulation import (
    ScenarioInformation as ScenarioInformationVO,
    ScenarioMetadata as ScenarioMetadataVO,
)
from app.routes.simulation.infra.models import (
    Group,
    OperationSetting,
    ScenarioInformation,
    ScenarioMetadata,
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

    # =====================================
    # 1. 시나리오 관리 (기본 CRUD 기능)
    # =====================================

    async def fetch_scenario_information(
        self,
        db: AsyncSession,
        user_id: str,
    ):
        """시나리오 목록 조회 (마스터/사용자 시나리오 구분)"""
        async with db.begin():
            # 현재 유저의 group_id 조회
            result = await db.execute(
                select(UserInformation.group_id).where(
                    UserInformation.user_id == user_id
                )
            )
            user_group_id = result.scalar_one_or_none()
            if not user_group_id:
                return {"master_scenario": [], "user_scenario": []}

            # 마스터 시나리오 ID 조회
            result = await db.execute(
                select(Group.master_scenario_id).where(Group.id == user_group_id)
            )
            master_scenario_id = result.scalar_one_or_none()

            # 단일 쿼리로 모든 시나리오 조회 (마스터 여부 구분)
            result = await db.execute(
                text(
                    """
                    SELECT 
                        si.*,
                        CASE WHEN si.scenario_id = :master_scenario_id THEN true ELSE false END as is_master
                    FROM scenario_information si
                    JOIN user_information ui ON si.user_id = ui.user_id
                    WHERE ui.group_id = :group_id 
                        AND si.is_active = true
                    ORDER BY si.updated_at DESC
                    LIMIT 50
                """
                ),
                {"group_id": user_group_id, "master_scenario_id": master_scenario_id},
            )

            # 결과를 마스터/사용자 시나리오로 분리
            master_scenarios = []
            user_scenarios = []

            for row in result.mappings():
                # is_master 필드를 제외한 시나리오 데이터 추출
                scenario_dict = {k: v for k, v in row.items() if k != "is_master"}

                if row["is_master"]:
                    master_scenarios.append(scenario_dict)
                else:
                    user_scenarios.append(scenario_dict)

            return {
                "master_scenario": master_scenarios,
                "user_scenario": user_scenarios,
            }

    async def create_scenario_information(
        self,
        db: AsyncSession,
        scenario_information: ScenarioInformationVO,
        scenario_metadata: ScenarioMetadataVO,
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
        await db.flush()

        new_metadata = ScenarioMetadata(
            scenario_id=new_scenario.scenario_id,
            overview=scenario_metadata.overview,
            history=scenario_metadata.history,
            flight_schedule=scenario_metadata.flight_schedule,
            passenger_schedule=scenario_metadata.passenger_schedule,
            processing_procedures=scenario_metadata.processing_procedures,
            facility_connection=scenario_metadata.facility_connection,
            facility_information=scenario_metadata.facility_information,
        )

        db.add(new_metadata)
        await db.commit()

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

    async def update_master_scenario(
        self, db: AsyncSession, group_id: int, scenario_id: str
    ):
        """마스터 시나리오 설정"""
        await db.execute(
            update(Group)
            .where(Group.id == group_id)
            .values({Group.master_scenario_id: scenario_id})
        )
        await db.commit()

    # =====================================
    # 2. 항공편 스케줄 처리 (Flight Schedule)
    # =====================================

    async def update_scenario_target_flight_schedule_date(
        self,
        db: AsyncSession,
        scenario_id: str,
        target_flight_schedule_date,
    ):
        """시나리오 대상 항공편 스케줄 날짜 업데이트"""
        # 문자열 날짜를 datetime 객체로 변환
        if isinstance(target_flight_schedule_date, str):
            target_date = datetime.strptime(target_flight_schedule_date, "%Y-%m-%d")
        else:
            target_date = target_flight_schedule_date

        await db.execute(
            update(ScenarioInformation)
            .where(ScenarioInformation.scenario_id == scenario_id)
            .values({ScenarioInformation.target_flight_schedule_date: target_date})
        )
        await db.commit()

    # =====================================
    # 3. 승객 스케줄 처리 (Show-up Passenger)
    # =====================================

    async def fetch_processing_procedures(self):
        """기본 프로세싱 절차 조회"""
        default_procedures = {
            "process": [
                {
                    "name": "Check-In",
                    "nodes": ["A", "B", "C", "D", "E", "F", "G", "H", "J", "K"],
                },
                {"name": "Boarding Pass", "nodes": ["DG1", "DG2", "DG3", "DG4"]},
                {"name": "Security", "nodes": ["SC1", "SC2", "SC3", "SC4"]},
                {"name": "Passport", "nodes": ["PC1", "PC2", "PC3", "PC4"]},
            ]
        }

        return default_procedures

    # =====================================
    # 4. 시뮬레이션 상태 및 권한 관리
    # =====================================

    async def update_simulation_start_end_at(
        self, db: AsyncSession, scenario_id: str, column: str, time
    ):
        """시뮬레이션 시작/종료 시간 업데이트

        Args:
            db (AsyncSession): Database session.
            scenario_id (str): Scenario ID to update.
            column (str): Column to update ('start' or 'end' or 'error').
            time: The time to set.

        Raises:
            ValueError: If column is not 'start' or 'end' or 'error', or if scenario_id is invalid.
            Exception: If database operation fails.
        """
        # Input validation
        if not scenario_id or not scenario_id.strip():
            raise ValueError("Scenario ID cannot be empty")

        if column not in ["start", "end", "error"]:
            raise ValueError("Invalid time parameter. Use 'start' or 'end' or 'error'")

        if not time:
            raise ValueError("Time parameter cannot be None")

        try:
            if column == "start":
                result = await db.execute(
                    update(ScenarioInformation)
                    .where(ScenarioInformation.scenario_id == scenario_id)
                    .values(
                        {
                            ScenarioInformation.status: "running",
                            ScenarioInformation.simulation_start_at: time,
                            ScenarioInformation.simulation_end_at: None,
                        }
                    )
                )

            elif column == "end":
                result = await db.execute(
                    update(ScenarioInformation)
                    .where(ScenarioInformation.scenario_id == scenario_id)
                    .values(
                        {
                            ScenarioInformation.status: "done",
                            ScenarioInformation.simulation_end_at: time,
                        }
                    )
                )

            elif column == "error":
                result = await db.execute(
                    update(ScenarioInformation)
                    .where(ScenarioInformation.scenario_id == scenario_id)
                    .values(
                        {
                            ScenarioInformation.status: "error",
                            ScenarioInformation.simulation_end_at: None,
                        }
                    )
                )

            # Check if any rows were affected
            if result.rowcount == 0:
                raise ValueError(f"No scenario found with ID: {scenario_id}")

            await db.commit()

        except ValueError:
            # Re-raise ValueError as is
            await db.rollback()
            raise
        except Exception as e:
            # Rollback transaction and re-raise with more context
            await db.rollback()
            raise Exception(
                f"Failed to update simulation time for scenario {scenario_id}: {str(e)}"
            ) from e

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
                # 사용자 권한까지 확인 (같은 그룹 내 시나리오인지 확인)
                result = await db.execute(
                    text(
                        """
                        SELECT COUNT(*) as count
                        FROM scenario_information si
                        JOIN user_information ui ON si.user_id = ui.user_id
                        WHERE si.scenario_id = :scenario_id 
                            AND ui.group_id = (
                                SELECT group_id 
                                FROM user_information 
                                WHERE user_id = :current_user_id
                            )
                            AND si.is_active = true
                    """
                    ),
                    {"scenario_id": scenario_id, "current_user_id": user_id},
                )
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

    async def fetch_scenario_location(
        self,
        db: AsyncSession,
        group_id: str,
    ):
        """시나리오 위치(터미널) 정보 조회"""
        async with db.begin():
            result = await db.execute(
                select(OperationSetting.terminal_name).where(
                    OperationSetting.group_id == int(group_id)
                )
            )
            scenario_info = [row["terminal_name"] for row in result.mappings().all()]
            scenario_info.append("Un-decided")

        return scenario_info
