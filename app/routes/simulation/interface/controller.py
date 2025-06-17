from operator import itemgetter

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, BackgroundTasks, Depends, Request, status
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncSession

from app.libs.containers import Container
from app.libs.exceptions import BadRequestException
from app.routes.simulation.application.service import SimulationService
from app.routes.simulation.interface.schema import (
    DuplicateScenarioBody,
    FacilityConnBody,
    FlightScheduleBody,
    PassengerScheduleBody,
    RunSimulationBody,
    ScenarioDeactivateBody,
    ScenarioMetadataBody,
    ScenarioUpdateBody,
    SetOpeningHoursBody,
    SimulationScenarioBody,
)
from packages.database import aget_supabase_session, get_snowflake_session

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
    "/scenarios/group-id/{group_id}",
    status_code=status.HTTP_200_OK,
    summary="06_SI_001",
    description="06_SI_001에서 각 유저가 가지고 있는 시나리오 리스트를 디비에서 불러오는 엔드포인트",
)
@inject
async def fetch_scenario(
    request: Request,
    group_id: str,
    page: int,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):

    if not group_id:
        raise BadRequestException("Group ID is required")

    return await sim_service.fetch_scenario_information(
        db=db,
        user_id=request.state.user_id,
        group_id=group_id,
        page=page,
        items_per_page=9,
    )


# @simulation_router.get(
#     "/scenarios/location/group-id/{group_id}",
#     status_code=status.HTTP_201_CREATED,
#     summary="06_SI_001",
#     description="06_SI_001에서 new_scenario 버튼을 클릭해서 나오는 팝업창에서 빈칸을 작성한 후 create 버튼을 누르면 실행되는 엔드포인트",
# )
# @inject
# async def fetch_scenario_location(
#     group_id: str,
#     sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
#     db: AsyncSession = Depends(aget_supabase_session),
# ):
#     if not group_id:
#         raise BadRequestException("Group ID is required")

#     return await sim_service.fetch_scenario_location(db=db, group_id=group_id)


@simulation_router.post(
    "/scenarios",
    status_code=status.HTTP_201_CREATED,
    summary="06_SI_001",
    description="06_SI_001에서 new_scenario 버튼을 클릭해서 나오는 팝업창에서 빈칸을 작성한 후 create 버튼을 누르면 실행되는 엔드포인트",
)
@inject
async def create_scenario(
    scenario: SimulationScenarioBody,
    request: Request,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):

    return await sim_service.create_scenario_information(
        db=db,
        user_id=request.state.user_id,
        name=scenario.name,
        editor=scenario.editor,
        terminal=scenario.terminal,
        airport=scenario.airport,
        memo=scenario.memo,
    )


@simulation_router.patch(
    "/scenarios/scenario-id/{scenario_id}/edit",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="06_SI_001",
    description="06_SI_001에서 각 시나리오의 액션버튼을 눌러 나오는 edit을 클릭하여 실행하면 실행되는 앤드포인트",
)
@inject
async def update_scenario(
    scenario_id: str,
    scenario: ScenarioUpdateBody,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    if not scenario_id:
        raise BadRequestException("Scenario ID is required")

    await sim_service.update_scenario_information(
        db=db,
        id=scenario_id,
        name=scenario.name,
        memo=scenario.memo,
    )


@simulation_router.patch(
    "/scenarios/deactivate",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="06_SI_001",
    description="06_SI_001에서 각 시나리오의 액션버튼을 눌러 나오는 delete를 클릭하여 실행하면 실행되는 앤드포인트",
)
@inject
async def deactivate_scenario(
    scenario_ids: ScenarioDeactivateBody,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):

    await sim_service.deactivate_scenario_information(db=db, ids=scenario_ids)


# @simulation_router.post(
#     "/scenarios/scenario-id/{scenario_id}/duplicate",
#     status_code=status.HTTP_204_NO_CONTENT,
#     summary="06_SI_001",
#     description="06_SI_001에서 각 시나리오의 액션버튼을 눌러 나오는 duplicate를 클릭하여 실행하면 실행되는 앤드포인트",
# )
# @inject
# async def duplicate_scenario(
#     request: Request,
#     scenario_id: str,
#     scenario: DuplicateScenarioBody,
#     sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
#     db: AsyncSession = Depends(aget_supabase_session),
# ):
#     if not scenario_id:
#         raise BadRequestException("Scenario ID is required")

#     await sim_service.duplicate_scenario_information(
#         db=db,
#         user_id=request.state.user_id,
#         old_scenario_id=scenario_id,
#         editor=scenario.editor,
#     )


@simulation_router.patch(
    "/scenarios/masters/group-id/{group_id}/scenario-id/{scenario_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="06_SI_001",
    description="06_SI_001에서 각 시나리오의 액션버튼을 눌러 나오는 master(미정)를 클릭하여 실행하면 실행되는 앤드포인트",
)
@inject
async def update_master_scenario(
    group_id: str,
    scenario_id: str,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):

    if not group_id or not scenario_id:
        raise BadRequestException("Group ID and Scenario ID is required")

    return await sim_service.update_master_scenario(
        db=db, group_id=group_id, scenario_id=scenario_id
    )


@simulation_router.patch(
    "/scenarios/scenario-id/{scenario_id}/status",
    status_code=204,
    summary="시나리오의 시뮬레이션 상태를 업데이트하는 엔드포인트",
    description="status에 들어갈 수 있는 값은 오직 'yet'과 'done'",
)
@inject
async def update_scenario_status(
    scenario_id: str,
    status: str,
    simulation_service: SimulationService = Depends(
        Provide[Container.simulation_service]
    ),
    db: Connection = Depends(aget_supabase_session),
):
    if not status or not scenario_id:
        raise BadRequestException("Scenario ID and Status is required")

    if status not in ["yet", "done"]:
        raise BadRequestException("status is only 'yet' or 'done'")

    return await simulation_service.update_scenario_status(
        db=db, scenario_id=scenario_id, status=status
    )


# ==============================
# NOTE: 시나리오 메타데이터


@simulation_router.get(
    "/scenarios/metadatas/scenario-id/{scenario_id}",
    status_code=status.HTTP_200_OK,
    summary="06_SI_001",
    description="06_SI_001에서 이미 존재하는 시나리오의 데이터를 불러오는 엔드포인트",
)
@inject
async def fetch_scenario_metadata(
    scenario_id: str,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):

    if not scenario_id:
        raise BadRequestException("Scenario ID is required")

    return await sim_service.fetch_scenario_metadata(db=db, scenario_id=scenario_id)


@simulation_router.put(
    "/scenarios/metadatas/scenario-id/{scenario_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="06_SI_002 ~ 021",
    description="06_SI_002 ~ 021에서 우상단의 save버튼을 눌렀을때 각 항목의 필터값들을 저장하는 엔드포인트",
)
@inject
async def update_scenario_metadata(
    scenario_id: str,
    metadata: ScenarioMetadataBody,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    if not scenario_id:
        raise BadRequestException("Scenario ID is required")

    return await sim_service.update_scenario_metadata(
        db,
        scenario_id,
        metadata.overview,
        metadata.history,
        metadata.flight_schedule,
        metadata.passenger_schedule,
        metadata.processing_procedures,
        metadata.facility_connection,
        metadata.facility_information,
    )


# ==============================
# NOTE: 시뮬레이션 프로세스
@simulation_router.post(
    "/flight-schedules/scenario-id/{scenario_id}",
    status_code=status.HTTP_200_OK,
    summary="06_SI_003, 06_SI_006",
    description="06_SI_003에서 LOAD 버튼과 06_SI_006에서 Apply 버튼을 눌렀을 때 실행되는 엔드포인트",
)
@inject
async def fetch_flight_schedule(
    scenario_id: str,
    flight_schedule: FlightScheduleBody,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    snowflake_db: Connection = Depends(get_snowflake_session),
    supabase_db: AsyncSession = Depends(aget_supabase_session),
):

    if not scenario_id:
        raise BadRequestException("Scenario ID is required")

    flight_sch = await sim_service.generate_flight_schedule(
        snowflake_db,
        flight_schedule.date,
        flight_schedule.airport,
        flight_schedule.condition,
        scenario_id=scenario_id,
    )

    await sim_service.update_scenario_target_flight_schedule_date(
        supabase_db, scenario_id, flight_schedule.date
    )

    return flight_sch


@simulation_router.post(
    "/passenger-schedules/scenario-id/{scenario_id}",
    status_code=status.HTTP_200_OK,
    summary="06_SI_009",
    description="06_SI_009에서 Apply 버튼을 눌렀을 때 실행되는 엔드포인트",
)
@inject
async def generate_passenger_schedule(
    scenario_id: str,
    passenger_schedule: PassengerScheduleBody,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: Connection = Depends(get_snowflake_session),
):
    return await sim_service.generate_passenger_schedule(
        db=db,
        flight_sch=passenger_schedule.flight_schedule,
        destribution_conditions=passenger_schedule.destribution_conditions,
        scenario_id=scenario_id,
    )


@simulation_router.post(
    "/processing-procedures",
    status_code=status.HTTP_200_OK,
    summary="06_SI_012",
    description="06_SI_012에서 설정한 운영세팅의 정보를 가져오는 엔드포인트",
)
@inject
async def fetch_processing_procedures(
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: Connection = Depends(get_snowflake_session),
):

    return await sim_service.fetch_processing_procedures()


@simulation_router.post(
    "/facility-conns/scenario-id/{scenario_id}",
    status_code=status.HTTP_200_OK,
    summary="06_SI_013",
    description="06_SI_013에서 최종 Apply 버튼을 눌렀을 때 facility info에서 사용할 바차트 데이터가 나오는 엔드포인트",
)
@inject
async def generate_facility_conn(
    scenario_id: str,
    facility_conn: FacilityConnBody,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: Connection = Depends(get_snowflake_session),
):

    return await sim_service.generate_facility_conn(
        facility_conn.processes, scenario_id
    )


@simulation_router.post(
    "/facility-info/charts/line",
    status_code=status.HTTP_200_OK,
    summary="06_SI_015",
    description="06_SI_015에서 set_opening_hours의 apply버튼을 눌렀을 때 line chart 데이터를 응답하는 엔드포인트",
)
@inject
async def generate_set_opening_hours(
    facility_info: SetOpeningHoursBody,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
):

    return await sim_service.generate_set_opening_hours(facility_info=facility_info)


@simulation_router.post(
    "/run-simulation/overview/scenario-id/{scenario_id}",
    status_code=status.HTTP_200_OK,
    summary="06_SI_018",
    description="06_SI_018로 들어올때 overview 화면에서 필요한 데이터를 응답하는 엔드포인트",
)
@inject
async def generate_simulation_overview(
    scenario_id: str,
    run_simulation: RunSimulationBody,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: Connection = Depends(get_snowflake_session),
):

    return await sim_service.generate_simulation_overview(
        db,
        run_simulation.flight_schedule,
        run_simulation.destribution_conditions,
        run_simulation.processes,
        run_simulation.components,
        scenario_id=scenario_id,
    )


@simulation_router.post(
    "/request-simulation/scenario-id/{scenario_id}", status_code=status.HTTP_200_OK
)
@inject
async def request_simulation(
    scenario_id: str,
    payload: RunSimulationBody,
    background_tasks: BackgroundTasks,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
):

    components, processes, flight_schedule = itemgetter(
        "components", "processes", "flight_schedule"
    )(payload.model_dump())

    return await sim_service.execute_simulation_by_scenario(
        schedule_date=flight_schedule.get("date"),
        scenario_id=scenario_id,
        components=components,
        processes=processes,
        background_tasks=background_tasks,
    )
