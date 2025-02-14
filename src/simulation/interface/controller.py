from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from supabase._async.client import AsyncClient as Client
from sqlalchemy import Connection

from src.simulation.interface.schema import (
    SimulationScenarioBody,
    FlightScheduleBody,
    PassengerScheduleBody,
    FacilityConnBody,
    RunSimulationBody,
    ScenarioMetadataBody,
)
from src.containers import Container
from src.database import get_snowflake_session, aget_supabase_session

from src.simulation.application.service import SimulationService

simulation_router = APIRouter(prefix="/simulations")


@simulation_router.post("/scenario", status_code=201)
@inject
async def create_scenario(
    scenario: SimulationScenarioBody,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    db: AsyncSession = Depends(aget_supabase_session),
):

    return await simulation_service.create_simulation_scenario(
        db,
        scenario.user_id,
        scenario.simulation_name,
        scenario.memo,
        scenario.terminal,
        scenario.editor,
    )


@simulation_router.get("/scenario", status_code=200)
@inject
async def fetch_scenario(
    user_id: str,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    db: AsyncSession = Depends(aget_supabase_session),
):
    return await simulation_service.fetch_simulation_scenario(db, user_id)


@simulation_router.get("/scenario/metadata", status_code=200)
@inject
async def fetch_scenario_metadata(
    simulation_id: str,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    db: AsyncSession = Depends(aget_supabase_session),
):
    return await simulation_service.fetch_scenario_metadata(db, simulation_id)


@simulation_router.post("/scenario/metadata", status_code=201)
@inject
async def update_scenario_metadata(
    metadata: ScenarioMetadataBody,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    db: AsyncSession = Depends(aget_supabase_session),
):

    return await simulation_service.update_scenario_metadata(
        db,
        metadata.simulation_id,
        metadata.overview,
        metadata.history,
        metadata.flight_sch,
        metadata.passenger_sch,
        metadata.passenger_attr,
        metadata.facility_conn,
        metadata.facility_info,
    )


@simulation_router.post("/flight_schedule", status_code=201)
@inject
async def fetch_flight_schedule_chart(
    flight_schedule: FlightScheduleBody,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    snowflake_db: Connection = Depends(get_snowflake_session),
    supabase_db: AsyncSession = Depends(aget_supabase_session),
):

    flight_sch = await simulation_service.generate_flight_schedule(
        snowflake_db,
        flight_schedule.date,
        flight_schedule.airport,
        flight_schedule.condition,
        flight_schedule.first_load,
    )

    await simulation_service.update_simulation_scenario(
        supabase_db, flight_schedule.user_id, flight_schedule.date
    )

    return flight_sch


@simulation_router.post("/passenger_schedule", status_code=201)
@inject
async def generate_passenger_schedule_chart(
    passenger_schedule: PassengerScheduleBody,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    db: Connection = Depends(get_snowflake_session),
):

    return await simulation_service.generate_passenger_schedule(
        db,
        passenger_schedule.flight_schedule,
        passenger_schedule.destribution_conditions,
    )


@simulation_router.post("/facility_conn", status_code=201)
@inject
async def generate_facility_conn(
    facility_conn: FacilityConnBody,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    db: Connection = Depends(get_snowflake_session),
):

    return await simulation_service.generate_facility_conn(
        db,
        facility_conn.flight_schedule,
        facility_conn.destribution_conditions,
        facility_conn.processes,
    )


@simulation_router.post("/run_simulation", status_code=201)
@inject
async def run_simulation(
    run_simulation: RunSimulationBody,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    db: Connection = Depends(get_snowflake_session),
):

    return await simulation_service.run_simulation(
        db,
        run_simulation.flight_schedule,
        run_simulation.destribution_conditions,
        run_simulation.processes,
        run_simulation.components,
    )
