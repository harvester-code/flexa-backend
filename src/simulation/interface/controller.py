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


@simulation_router.post(
    "/scenario",
    status_code=201,
    summary="06_SI_001",
    description="06_SI_001에서 new_scenario 버튼을 클릭해서 나오는 팝업창에서 빈칸을 작성한 후 create 버튼을 누르면 실행되는 API",
)
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


@simulation_router.get(
    "/scenario",
    status_code=200,
    summary="06_SI_001",
    description="06_SI_001에서 각 유저가 가지고 있는 시나리오 리스트를 디비에서 불러오는 API",
)
@inject
async def fetch_scenario(
    user_id: str,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    db: AsyncSession = Depends(aget_supabase_session),
):
    return await simulation_service.fetch_simulation_scenario(db, user_id)


@simulation_router.get(
    "/scenario/metadata",
    status_code=200,
    summary="06_SI_001",
    description="06_SI_001에서 이미 존재하는 시나리오의 데이터를 불러오는 API",
)
@inject
async def fetch_scenario_metadata(
    simulation_id: str,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    db: AsyncSession = Depends(aget_supabase_session),
):
    return await simulation_service.fetch_scenario_metadata(db, simulation_id)


@simulation_router.post(
    "/scenario/metadata",
    status_code=201,
    summary="06_SI_002 ~ 021",
    description="06_SI_002 ~ 021에서 우상단의 save버튼을 눌렀을때 각 항목의 필터값들을 저장하는 API",
)
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


@simulation_router.post(
    "/flight-schedule",
    status_code=201,
    summary="06_SI_003, 06_SI_006",
    description="06_SI_003에서 LOAD 버튼과 06_SI_006에서 Apply 버튼을 눌렀을 때 실행되는 API /// 만약 body값에 first_load: true로 설정이 되어있면, add_conditions에 필요한 데이터를 전달한다.",
)
@inject
async def fetch_flight_schedule(
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

    # await simulation_service.update_simulation_scenario(
    #     supabase_db, flight_schedule.user_id, flight_schedule.date
    # )

    return flight_sch


@simulation_router.post(
    "/passenger-schedule",
    status_code=201,
    summary="06_SI_009",
    description="06_SI_009에서 Apply 버튼을 눌렀을 때 실행되는 API",
)
@inject
async def generate_passenger_schedule(
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


@simulation_router.post("/facility-conn", status_code=201)
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


@simulation_router.post("/run-simulation", status_code=201)
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
