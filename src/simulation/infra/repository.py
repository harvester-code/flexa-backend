import awswrangler as wr
import boto3
import pandas as pd

from typing import Union, List
from sqlalchemy import Connection, update, true, desc, bindparam
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.inspection import inspect

from src.database import S3_SAVE_PATH
from src.simulation.domain.repository import ISimulationRepository
from src.simulation.domain.simulation import (
    ScenarioMetadata as ScenarioMetadataVO,
)
from src.simulation.domain.simulation import (
    SimulationScenario as SimulationScenarioVO,
)
from src.simulation.infra.models import ScenarioMetadata, SimulationScenario, Groups
from src.simulation.infra.schema import (
    GeneralDeclarationArrival,
    GeneralDeclarationDeparture,
)


class SimulationRepository(ISimulationRepository):

    # ===================================
    # NOTE: 시뮬레이션 시나리오

    async def fetch_simulation_scenario(
        self, db: AsyncSession, user_id: str, group_id: str
    ):

        async with db.begin():
            result = await db.execute(
                select(Groups.master_scenario_id).where(Groups.id == int(group_id))
            )

            master_scenario_id = result.scalar_one_or_none()

            master_scenario = None
            if master_scenario_id:
                result = await db.execute(
                    select(SimulationScenario)
                    .where(SimulationScenario.id == master_scenario_id)
                    .where(SimulationScenario.is_active.is_(true()))
                )

                master_scenario = result.scalar_one_or_none()

            result = await db.execute(
                select(SimulationScenario)
                .where(SimulationScenario.user_id == user_id)
                .where(SimulationScenario.is_active.is_(true()))
                .order_by(desc(SimulationScenario.updated_at))
            )

            user_scenario = result.scalars().all()

        return {"master_scenario": [master_scenario], "user_scenario": user_scenario}

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
            memo=simulation_scenario.memo,
            simulation_date=simulation_scenario.simulation_date,
            updated_at=simulation_scenario.updated_at,
            created_at=simulation_scenario.created_at,
        )

        db.add(new_scenario)
        await db.flush()

        new_metadata = ScenarioMetadata(
            scenario_id=new_scenario.id,
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
        self, db: AsyncSession, id: str, name: str | None, memo: str | None
    ):
        values_to_update = {}

        if name:
            values_to_update[SimulationScenario.simulation_name] = name
        if memo:
            values_to_update[SimulationScenario.memo] = memo

        await db.execute(
            update(SimulationScenario)
            .where(SimulationScenario.id == id)
            .values(values_to_update)
        )
        await db.commit()

    async def deactivate_simulation_scenario(
        self, db: AsyncSession, id: Union[str, List[str]]
    ):

        if isinstance(id, str):
            id_list = [id]

        else:
            id_list = id

        await db.execute(
            update(SimulationScenario)
            .where(SimulationScenario.id.in_(id_list))
            .values({SimulationScenario.is_active: False})
        )
        await db.commit()

    async def duplicate_simulation_scenario(
        self,
        db: AsyncSession,
        user_id: str,
        old_id: str,
        new_id: str,
        editor: str,
        time_now,
    ):
        scenario_result = await db.execute(
            select(SimulationScenario)
            .where(SimulationScenario.id == old_id)
            .where(SimulationScenario.is_active.is_(true()))
        )

        origin_scenario = scenario_result.scalar_one()

        scenario_state = inspect(origin_scenario)

        scenario_data = {
            attr.key: getattr(origin_scenario, attr.key)
            for attr in scenario_state.mapper.column_attrs
            if attr.key not in ("id", "user_id", "editor", "updated_at", "created_at")
        }

        cloned_scenario = origin_scenario.__class__(**scenario_data)

        cloned_scenario.user_id = user_id
        cloned_scenario.id = new_id
        cloned_scenario.editor = editor
        cloned_scenario.updated_at = time_now
        cloned_scenario.created_at = time_now
        db.add(cloned_scenario)
        await db.flush()

        metadata_result = await db.execute(
            select(ScenarioMetadata).where(ScenarioMetadata.scenario_id == old_id)
        )

        origin_metadata = metadata_result.scalar_one()

        metadata_state = inspect(origin_metadata)

        metadata_data = {
            attr.key: getattr(origin_metadata, attr.key)
            for attr in metadata_state.mapper.column_attrs
            if attr.key not in ("scenario_id")
        }

        cloned_metadata = origin_metadata.__class__(**metadata_data)

        cloned_metadata.scenario_id = cloned_scenario.id
        db.add(cloned_metadata)
        await db.commit()

    async def update_master_scenario(
        self, db: AsyncSession, group_id: str, scenario_id: str
    ):

        await db.execute(
            update(Groups)
            .where(Groups.id == int(group_id))
            .values({Groups.master_scenario_id: scenario_id})
        )
        await db.commit()

    # ===================================
    # NOTE: 시나리오 메타데이터

    async def fetch_scenario_metadata(self, db: AsyncSession, scenario_id: str):
        async with db.begin():

            result = await db.execute(
                select(
                    SimulationScenario.simulation_name,
                    SimulationScenario.memo,
                    SimulationScenario.editor,
                    SimulationScenario.terminal,
                ).where(SimulationScenario.id == scenario_id)
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
        self, db: AsyncSession, scenario_metadata: ScenarioMetadataVO
    ):

        result = await db.execute(
            select(ScenarioMetadata).where(
                ScenarioMetadata.scenario_id == scenario_metadata.scenario_id
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
        if params.get("airline"):
            stmt = stmt.bindparams(bindparam("airline", expanding=True))

        result = conn.execute(stmt, params)

        rows = [dict(schema_map.get(flight_io)(**row._mapping)) for row in result]
        return rows

    async def update_simulation_scenario_target_date(
        self,
        db: AsyncSession,
        scenario_id: str,
        target_datetime,
    ):

        await db.execute(
            update(SimulationScenario)
            .where(SimulationScenario.id == scenario_id)
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
