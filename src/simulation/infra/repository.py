import awswrangler as wr
import boto3
import pandas as pd
from sqlalchemy import Connection, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from src.database import S3_SAVE_PATH
from src.simulation.domain.repository import ISimulationRepository
from src.simulation.domain.simulation import (
    ScenarioMetadata as ScenarioMetadataVO,
)
from src.simulation.domain.simulation import (
    SimulationScenario as SimulationScenarioVO,
)
from src.simulation.infra.models import ScenarioMetadata, SimulationScenario
from src.simulation.infra.schema import (
    GeneralDeclarationArrival,
    GeneralDeclarationDeparture,
)


class SimulationRepository(ISimulationRepository):

    # ===================================
    # NOTE: 시뮬레이션 시나리오

    async def fetch_simulation_scenario(self, db: AsyncSession, user_id: str):

        result = await db.execute(
            select(SimulationScenario)
            .where(SimulationScenario.user_id == user_id)
            .where(SimulationScenario.is_active is True)
        )

        scenario = result.scalars().all()

        return scenario

    async def create_simulation_scenario(
        self,
        db: AsyncSession,
        simulation_scenario: SimulationScenarioVO,
        scenario_metadata: ScenarioMetadataVO,
    ):

        new_scenario = SimulationScenario(
            id=simulation_scenario.id,
            user_id=simulation_scenario.user_id,
            simulation_url=simulation_scenario.simulation_url,
            simulation_name=simulation_scenario.simulation_name,
            size=simulation_scenario.size,
            terminal=simulation_scenario.terminal,
            editor=simulation_scenario.editor,
            note=simulation_scenario.note,
            simulation_date=simulation_scenario.simulation_date,
            updated_at=simulation_scenario.updated_at,
            created_at=simulation_scenario.created_at,
        )

        db.add(new_scenario)
        await db.flush()

        new_metadata = ScenarioMetadata(
            simulation_id=new_scenario.id,
            overview=scenario_metadata.overview,
            history=scenario_metadata.history,
            flight_sch=scenario_metadata.flight_sch,
            passenger_sch=scenario_metadata.passenger_sch,
            passenger_attr=scenario_metadata.passenger_attr,
            facility_conn=scenario_metadata.facility_conn,
            facility_info=scenario_metadata.facility_info,
        )

        db.add(new_metadata)
        await db.commit()

    async def update_simulation_scenario(
        self, db: AsyncSession, id: str, name: str | None, note: str | None
    ):
        values_to_update = {}

        if name:
            values_to_update[SimulationScenario.simulation_name] = name
        if note:
            values_to_update[SimulationScenario.note] = note

        await db.execute(
            update(SimulationScenario)
            .where(SimulationScenario.id == id)
            .values(values_to_update)
        )
        await db.commit()

    async def deactivate_simulation_scenario(self, db: AsyncSession, id: str):

        await db.execute(
            update(SimulationScenario)
            .where(SimulationScenario.id == id)
            .values({SimulationScenario.is_active: False})
        )
        await db.commit()

    # ===================================
    # NOTE: 시나리오 메타데이터

    async def fetch_scenario_metadata(self, db: AsyncSession, simulation_id: str):

        result = await db.execute(
            select(ScenarioMetadata).where(
                ScenarioMetadata.simulation_id == simulation_id
            )
        )

        metadata = result.scalar_one_or_none()

        return metadata

    async def update_scenario_metadata(
        self, db: AsyncSession, scenario_metadata: ScenarioMetadataVO
    ):

        result = await db.execute(
            select(ScenarioMetadata).where(
                ScenarioMetadata.simulation_id == scenario_metadata.simulation_id
            )
        )

        metadata = result.scalars().first()

        if metadata:
            metadata.overview = scenario_metadata.overview
            metadata.history = scenario_metadata.history
            metadata.flight_sch = scenario_metadata.flight_sch
            metadata.passenger_sch = scenario_metadata.passenger_sch
            metadata.passenger_attr = scenario_metadata.passenger_attr
            metadata.facility_conn = scenario_metadata.facility_conn
            metadata.facility_info = scenario_metadata.facility_info

            await db.commit()

    # ===================================
    # NOTE: 시뮬레이션 프로세스

    async def fetch_flight_schedule_data(
        self, conn: Connection, stmt, params, flight_io
    ):

        schema_map = {
            "arrival": GeneralDeclarationArrival,
            "departure": GeneralDeclarationDeparture,
        }

        result = conn.execute(stmt, params)

        rows = [dict(schema_map.get(flight_io)(**row._mapping)) for row in result]
        return rows

    async def update_simulation_scenario_target_date(
        self,
        db: AsyncSession,
        id: str,
        target_datetime,
    ):

        await db.execute(
            update(SimulationScenario)
            .where(SimulationScenario.id == id)
            .values({SimulationScenario.simulation_date: target_datetime})
        )
        await db.commit()

    async def upload_to_s3(
        self, session: boto3.Session, sim_df: pd.DataFrame, filename: str
    ):

        wr.s3.to_parquet(
            df=sim_df,
            path=f"{S3_SAVE_PATH}/{filename}",
            boto3_session=session,
        )

    async def download_from_s3(
        self, session: boto3.Session, filename: str
    ) -> pd.DataFrame:

        sim_df = wr.s3.read_parquet(
            path=f"{S3_SAVE_PATH}/{filename}", boto3_session=session
        )

        return sim_df
