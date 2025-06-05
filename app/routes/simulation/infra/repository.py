from typing import List

from sqlalchemy import Connection, bindparam, desc, func, true, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.inspection import inspect

from app.routes.simulation.domain.repository import ISimulationRepository
from app.routes.simulation.domain.simulation import (
    ScenarioMetadata as ScenarioMetadataVO,
)
from app.routes.simulation.domain.simulation import (
    ScenarioInformation as ScenarioInformationVO,
)
from app.routes.simulation.infra.models import (
    Group,
    OperationSetting,
    ScenarioMetadata,
    ScenarioInformation,
)
from app.routes.simulation.infra.schema import (
    GeneralDeclarationArrival,
    GeneralDeclarationDeparture,
)


class SimulationRepository(ISimulationRepository):

    # ===================================
    # NOTE: 시뮬레이션 시나리오

    async def fetch_scenario_information(
        self,
        db: AsyncSession,
        user_id: str,
        group_id: str,
        page: int,
        items_per_page: int,
    ):

        # FIXME: [25.04.07] 여기서 DB를 호출하는 횟수를 최대한 줄여보자
        async with db.begin():
            result = await db.execute(
                select(Group.master_scenario_id).where(Group.id == int(group_id))
            )

            master_scenario_id = result.scalar_one_or_none()

            master_scenario = None
            if master_scenario_id:
                result = await db.execute(
                    select(ScenarioInformation)
                    .where(ScenarioInformation.id == master_scenario_id)
                    .where(ScenarioInformation.is_active.is_(true()))
                )

                master_scenario = result.scalar_one_or_none()

            result = await db.execute(
                select(func.count())
                .select_from(ScenarioInformation)
                .where(ScenarioInformation.user_id == user_id)
                .where(ScenarioInformation.is_active.is_(true()))
            )
            total_count = result.scalar()

            offset = (page - 1) * items_per_page

            result = await db.execute(
                select(ScenarioInformation)
                .where(ScenarioInformation.user_id == user_id)
                .where(ScenarioInformation.is_active.is_(true()))
                .order_by(desc(ScenarioInformation.updated_at))
                .offset(offset)
                .limit(items_per_page)
            )

            user_scenario = result.scalars().all()

        return {
            "total_count": total_count,
            "page": page,
            "master_scenario": [master_scenario],
            "user_scenario": user_scenario,
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
            id=scenario_information.id,
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
            scenario_id=new_scenario.id,
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
        self, db: AsyncSession, id: str, name: str | None, memo: str | None
    ):
        values_to_update = {}

        if name:
            values_to_update[ScenarioInformation.name] = name
        if memo:
            values_to_update[ScenarioInformation.memo] = memo

        await db.execute(
            update(ScenarioInformation)
            .where(ScenarioInformation.id == id)
            .values(values_to_update)
        )
        await db.commit()

    async def deactivate_scenario_information(self, db: AsyncSession, ids: List[str]):

        stmt = (
            update(ScenarioInformation)
            .where(ScenarioInformation.id.in_(bindparam("ids", expanding=True)))
            .values(is_active=False)
        )
        await db.execute(stmt, {"ids": ids.scenario_ids})
        await db.commit()

    async def duplicate_scenario_information(
        self,
        db: AsyncSession,
        user_id: str,
        old_id: str,
        new_id: str,
        editor: str,
        time_now,
    ):
        scenario_result = await db.execute(
            select(ScenarioInformation)
            .where(ScenarioInformation.id == old_id)
            .where(ScenarioInformation.is_active.is_(true()))
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
            update(Group)
            .where(Group.id == int(group_id))
            .values({Group.master_scenario_id: scenario_id})
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
                ).where(ScenarioInformation.id == scenario_id)
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
                .where(ScenarioInformation.id == scenario_metadata.scenario_id)
                .values({ScenarioInformation.updated_at: time_now})
            )

            await db.commit()

    # ===================================
    # NOTE: 시뮬레이션 프로세스

    async def fetch_flight_schedule_data(
        self, conn: Connection, stmt, params, flight_io
    ) -> List[dict]:
        schema_map = {
            "arrival": GeneralDeclarationArrival,
            "departure": GeneralDeclarationDeparture,
        }

        if params.get("airline"):
            stmt = stmt.bindparams(bindparam("airline", expanding=True))

        result = conn.execute(stmt, params)
        rows = [dict(schema_map.get(flight_io)(**row._mapping)) for row in result]
        return rows

    async def update_scenario_target_flight_schedule_date(
        self,
        db: AsyncSession,
        scenario_id: str,
        target_flight_schedule_date,
    ):
        await db.execute(
            update(ScenarioInformation)
            .where(ScenarioInformation.id == scenario_id)
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
