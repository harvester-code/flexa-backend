import boto3
from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Request
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncSession

from src.containers import Container
from src.database import aget_supabase_session, get_boto3_session, get_snowflake_session
from src.simulation.application.service import SimulationService
from src.simulation.interface.schema import (
    FacilityConnBody,
    FlightScheduleBody,
    PassengerScheduleBody,
    RunSimulationBody,
    ScenarioMetadataBody,
    ScenarioUpdateBody,
    SimulationScenarioBody,
    SimulationTotalChartBody,
)

simulation_router = APIRouter(prefix="/simulations")


"""
status 코드 정리
200: 요청이 성공적으로 처리되었고, 응답 본문에 업데이트된 데이터를 포함할 경우
201: 새로운 리소스를 생성할 때 사용
204: 요청이 성공적으로 처리되었지만, 응답 본문이 필요 없을 경우
400: 요청이 올바르지 않을 경우
"""

# ==============================
# NOTE: 시뮬레이션 시나리오


@simulation_router.get(
    "/scenario",
    status_code=200,
    summary="06_SI_001",
    description="06_SI_001에서 각 유저가 가지고 있는 시나리오 리스트를 디비에서 불러오는 엔드포인트",
)
@inject
async def fetch_scenario(
    request: Request,
    group_id: str,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    db: AsyncSession = Depends(aget_supabase_session),
):
    return await simulation_service.fetch_simulation_scenario(
        db, request.state.user_id, group_id
    )


@simulation_router.post(
    "/scenario",
    status_code=201,
    summary="06_SI_001",
    description="06_SI_001에서 new_scenario 버튼을 클릭해서 나오는 팝업창에서 빈칸을 작성한 후 create 버튼을 누르면 실행되는 엔드포인트",
)
@inject
async def create_scenario(
    scenario: SimulationScenarioBody,
    request: Request,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    db: AsyncSession = Depends(aget_supabase_session),
):

    return await simulation_service.create_simulation_scenario(
        db,
        request.state.user_id,
        scenario.simulation_name,
        scenario.note,
        scenario.terminal,
        scenario.editor,
    )


@simulation_router.patch(
    "/scenario",
    status_code=204,
    summary="06_SI_001",
    description="06_SI_001에서 각 시나리오의 액션버튼을 눌러 나오는 edit을 클릭하여 실행하면 실행되는 앤드포인트",
)
@inject
async def update_scenario(
    scenario: ScenarioUpdateBody,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    db: AsyncSession = Depends(aget_supabase_session),
):

    return await simulation_service.update_simulation_scenario(
        db,
        scenario.id,
        scenario.simulation_name,
        scenario.note,
    )


@simulation_router.patch(
    "/scenario/deactivate",
    status_code=204,
    summary="06_SI_001",
    description="06_SI_001에서 각 시나리오의 액션버튼을 눌러 나오는 delete를 클릭하여 실행하면 실행되는 앤드포인트",
)
@inject
async def deactivate_scenario(
    scenario_id: str,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    db: AsyncSession = Depends(aget_supabase_session),
):

    return await simulation_service.deactivate_simulation_scenario(
        db,
        scenario_id,
    )


@simulation_router.post(
    "/scenario/duplicate",
    status_code=204,
    summary="06_SI_001",
    description="06_SI_001에서 각 시나리오의 액션버튼을 눌러 나오는 duplicate를 클릭하여 실행하면 실행되는 앤드포인트",
)
@inject
async def duplicate_scenario(
    request: Request,
    scenario_id: str,
    editor: str,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    db: AsyncSession = Depends(aget_supabase_session),
):

    return await simulation_service.duplicate_simulation_scenario(
        db, request.state.user_id, scenario_id, editor
    )


@simulation_router.patch(
    "/scenario/master",
    status_code=204,
    summary="06_SI_001",
    description="06_SI_001에서 각 시나리오의 액션버튼을 눌러 나오는 master(미정)를 클릭하여 실행하면 실행되는 앤드포인트",
)
@inject
async def update_master_scenario(
    group_id: str,
    scenario_id: str,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    db: AsyncSession = Depends(aget_supabase_session),
):

    return await simulation_service.update_master_scenario(db, group_id, scenario_id)


# ==============================
# NOTE: 시나리오 메타데이터


@simulation_router.get(
    "/scenario/metadata",
    status_code=200,
    summary="06_SI_001",
    description="06_SI_001에서 이미 존재하는 시나리오의 데이터를 불러오는 엔드포인트",
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


@simulation_router.put(
    "/scenario/metadata",
    status_code=204,
    summary="06_SI_002 ~ 021",
    description="06_SI_002 ~ 021에서 우상단의 save버튼을 눌렀을때 각 항목의 필터값들을 저장하는 엔드포인트",
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


# ==============================
# NOTE: 시뮬레이션 프로세스
@simulation_router.post(
    "/flight-schedule",
    status_code=200,
    summary="06_SI_003, 06_SI_006",
    description="06_SI_003에서 LOAD 버튼과 06_SI_006에서 Apply 버튼을 눌렀을 때 실행되는 엔드포인트 /// 만약 body값에 first_load: true로 설정이 되어있면, add_conditions에 필요한 데이터를 전달한다.",
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

    # await simulation_service.update_simulation_scenario_target_date(
    #     supabase_db, flight_schedule.user_id, flight_schedule.date
    # )

    return flight_sch


@simulation_router.post(
    "/passenger-schedule",
    status_code=200,
    summary="06_SI_009",
    description="06_SI_009에서 Apply 버튼을 눌렀을 때 실행되는 엔드포인트",
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


@simulation_router.post("/facility-conn", status_code=200)
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


@simulation_router.post("/run-simulation", status_code=200)
@inject
async def run_simulation(
    run_simulation: RunSimulationBody,
    request: Request,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    db: Connection = Depends(get_snowflake_session),
    session: boto3.Session = Depends(get_boto3_session),
):

    return await simulation_service.run_simulation(
        db,
        session,
        request.state.user_id,
        run_simulation.scenario_id,
        run_simulation.flight_schedule,
        run_simulation.destribution_conditions,
        run_simulation.processes,
        run_simulation.components,
    )


@simulation_router.post("/kpi-chart", status_code=200)
@inject
async def generate_simulation_kpi_chart(
    scenario_id: str,
    process: str,
    node: str,
    request: Request,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    session: boto3.Session = Depends(get_boto3_session),
):

    return await simulation_service.generate_simulation_kpi_chart(
        session=session,
        user_id=request.state.user_id,
        scenario_id=scenario_id,
        process=process,
        node=node,
        sim_df=None,
    )


@simulation_router.post("/total-chart", status_code=200)
@inject
async def generate_simulation_total_chart(
    scenario_id: str,
    total: SimulationTotalChartBody,
    request: Request,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    session: boto3.Session = Depends(get_boto3_session),
):

    return await simulation_service.generate_simulation_total_chart(
        session=session,
        user_id=request.state.user_id,
        scenario_id=scenario_id,
        total=total.total,
    )
