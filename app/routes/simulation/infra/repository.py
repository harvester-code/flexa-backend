from typing import List

import redshift_connector
from pendulum import DateTime
from sqlalchemy import bindparam, text, true, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.inspection import inspect

from app.routes.simulation.domain.repository import ISimulationRepository
from app.routes.simulation.domain.simulation import (
    ScenarioInformation as ScenarioInformationVO,
)
from app.routes.simulation.domain.simulation import (
    ScenarioMetadata as ScenarioMetadataVO,
)
from app.routes.simulation.infra.models import (
    Group,
    OperationSetting,
    ScenarioInformation,
    ScenarioMetadata,
    UserInformation,
)
from app.routes.simulation.infra.schema import (
    GeneralDeclarationArrival,
    GeneralDeclarationDeparture,
)


class SimulationRepository(ISimulationRepository):

    async def fetch_scenario_information(
        self,
        db: AsyncSession,
        user_id: str,
    ):
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
                        CASE WHEN si.id = :master_scenario_id THEN true ELSE false END as is_master
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

    async def fetch_scenario_location(
        self,
        db: AsyncSession,
        group_id: str,
    ):

        async with db.begin():
            result = await db.execute(
                select(OperationSetting.terminal_name).where(
                    OperationSetting.group_id == int(group_id)
                )
            )
            scenario_info = [row["terminal_name"] for row in result.mappings().all()]
            scenario_info.append("Un-decided")

        return scenario_info

    async def create_scenario_information(
        self,
        db: AsyncSession,
        scenario_information: ScenarioInformationVO,
        scenario_metadata: ScenarioMetadataVO,
    ):

        new_scenario = ScenarioInformation(
            scenario_id=scenario_information.scenario_id,
            user_id=scenario_information.user_id,
            editor=scenario_information.editor,
            name=scenario_information.name,
            terminal=scenario_information.terminal,
            airport=scenario_information.airport,
            memo=scenario_information.memo,
            target_flight_schedule_date=scenario_information.target_flight_schedule_date,
            created_at=scenario_information.created_at,
            updated_at=scenario_information.updated_at,
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
        id: str,
        name: str | None,
        terminal: str | None,
        airport: str | None,
        memo: str | None,
    ):
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
            .where(ScenarioInformation.scenario_id == id)
            .values(values_to_update)
        )
        await db.commit()

    async def deactivate_scenario_information(self, db: AsyncSession, ids: List[str]):

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

        await db.execute(
            update(Group)
            .where(Group.id == group_id)
            .values({Group.master_scenario_id: scenario_id})
        )
        await db.commit()

    async def update_scenario_status(
        self, db: AsyncSession, scenario_id: str, status: str
    ):

        await db.execute(
            update(ScenarioInformation)
            .where(ScenarioInformation.scenario_id == scenario_id)
            .values({ScenarioInformation.status: status})
        )
        await db.commit()

    # ===================================
    # NOTE: 시나리오 메타데이터

    async def fetch_scenario_metadata(self, db: AsyncSession, scenario_id: str):
        async with db.begin():

            result = await db.execute(
                select(
                    ScenarioInformation.name,
                    ScenarioInformation.memo,
                    ScenarioInformation.editor,
                    ScenarioInformation.terminal,
                ).where(ScenarioInformation.scenario_id == scenario_id)
            )

            scenario_info = result.mappings().first()

            result = await db.execute(
                select(ScenarioMetadata).where(
                    ScenarioMetadata.scenario_id == scenario_id
                )
            )

            metadata = result.scalar_one_or_none()

        return {"scenario_info": scenario_info, "metadata": metadata}

    async def update_scenario_metadata(
        self, db: AsyncSession, scenario_metadata: ScenarioMetadataVO, time_now
    ):

        async with db.begin():

            result = await db.execute(
                select(ScenarioMetadata).where(
                    ScenarioMetadata.scenario_id == scenario_metadata.scenario_id
                )
            )

            metadata = result.scalars().first()

            if metadata:
                metadata.overview = scenario_metadata.overview
                metadata.history = scenario_metadata.history
                metadata.flight_schedule = scenario_metadata.flight_schedule
                metadata.passenger_schedule = scenario_metadata.passenger_schedule
                metadata.processing_procedures = scenario_metadata.processing_procedures
                metadata.facility_connection = scenario_metadata.facility_connection
                metadata.facility_information = scenario_metadata.facility_information

                await db.flush()

            result = await db.execute(
                update(ScenarioInformation)
                .where(ScenarioInformation.scenario_id == scenario_metadata.scenario_id)
                .values({ScenarioInformation.updated_at: time_now})
            )

            await db.commit()

    # ===================================
    # NOTE: 시뮬레이션 프로세스
    async def fetch_flight_schedule_data(
        self,
        conn: redshift_connector.core.Connection,
        stmt_text: str,
        params: List[str],
        flight_io: str,
    ):
        # NOTE: 추후 Snowflake로 변경시, 데이터소스를 분기하는 로직 추가할 것.
        """Fetch flight schedule data from Redshift.

        Args:
            conn (redshift_connector.core.Connection): Connection to the Redshift database.
            stmt_text (str): Query statement text to execute.
            params (List[str]): Parameters to bind to the query.
            flight_io (str): Flight I/O type, either 'arrival' or 'departure'.

        Returns:
            List[GeneralDeclarationArrival | GeneralDeclarationDeparture]: A list of flight schedule data
        """

        schema_map = {
            "arrival": GeneralDeclarationArrival,
            "departure": GeneralDeclarationDeparture,
        }

        try:
            with conn.cursor() as cursor:
                cursor.execute(stmt_text, params)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

            results = [dict(zip(columns, row)) for row in rows]
            return [dict(schema_map.get(flight_io)(**result)) for result in results]

        except redshift_connector.Error as e:
            print(f"Error executing query: {e}")
            return []

    async def update_scenario_target_flight_schedule_date(
        self,
        db: AsyncSession,
        scenario_id: str,
        target_flight_schedule_date,
    ):
        await db.execute(
            update(ScenarioInformation)
            .where(ScenarioInformation.scenario_id == scenario_id)
            .values(
                {
                    ScenarioInformation.target_flight_schedule_date: target_flight_schedule_date
                }
            )
        )
        await db.commit()

    async def fetch_processing_procedures(self):
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

    async def update_simulation_start_end_at(
        self, db: AsyncSession, scenario_id: str, column: str, time: DateTime
    ):
        """Update the start or end time of a simulation scenario.

        Args:
            db (AsyncSession): Database session.
            scenario_id (str): Scenario ID to update.
            column (str): Column to update ('start' or 'end' or 'error').
            time (DateTime): The time to set.

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
        result = await db.execute(
            select(1)
            .where(ScenarioInformation.scenario_id == scenario_id)
            .where(ScenarioInformation.user_id == user_id)
        )

        exists = result.scalar() is not None
        if not exists:
            raise ValueError("User does not have permission for this scenario")
