from operator import itemgetter

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from loguru import logger
from sqlalchemy import Connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.libs.containers import Container
from app.libs.dependencies import verify_token
from app.libs.exceptions import BadRequestException
from app.routes.simulation.application.service import SimulationService
from app.routes.simulation.interface.schema import (
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
from packages.database import aget_supabase_session, get_redshift_connection

private_simulation_router = APIRouter(
    prefix="/simulations", dependencies=[Depends(verify_token)]
)

# NOTE: Lambda 함수에서 인증이 필요 없는 엔드포인트를 위한 라우터
public_simulation_router = APIRouter(
    prefix="/simulations",
)


"""
status 코드 정리
200: 요청이 성공적으로 처리되었고, 응답 본문에 업데이트된 데이터를 포함할 경우
201: 새로운 리소스를 생성할 때 사용
204: 요청이 성공적으로 처리되었지만, 응답 본문이 필요 없을 경우
400: 요청이 올바르지 않을 경우
"""

# ==============================
# NOTE: 시뮬레이션 시나리오


@private_simulation_router.get(
    "/scenarios",
    status_code=status.HTTP_200_OK,
    summary="시나리오 목록 조회",
    description="현재 유저와 같은 그룹의 모든 시나리오를 조회합니다 (최대 50개)",
)
@inject
async def get_scenarios(
    request: Request,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    return await sim_service.fetch_scenario_information(
        db=db,
        user_id=request.state.user_id,
    )


@private_simulation_router.post(
    "/scenarios",
    status_code=status.HTTP_201_CREATED,
    summary="시나리오 생성",
    description="새로운 시나리오를 생성합니다",
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


@private_simulation_router.put(
    "/scenarios/{scenario_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="시나리오 수정",
    description="기존 시나리오의 정보를 수정합니다",
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
        terminal=scenario.terminal,
        airport=scenario.airport,
        memo=scenario.memo,
    )


@private_simulation_router.delete(
    "/scenarios",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="시나리오 삭제",
    description="시나리오들을 소프트 삭제합니다",
)
@inject
async def delete_scenarios(
    scenario_ids: ScenarioDeactivateBody,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    await sim_service.deactivate_scenario_information(
        db=db, ids=scenario_ids.scenario_ids
    )


@private_simulation_router.patch(
    "/scenarios/{scenario_id}/master",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="마스터 시나리오 설정",
    description="특정 시나리오를 그룹의 마스터 시나리오로 설정합니다",
)
@inject
async def update_master_scenario(
    request: Request,
    scenario_id: str,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    if not scenario_id:
        raise BadRequestException("Scenario ID is required")

    return await sim_service.update_master_scenario(
        db=db, user_id=request.state.user_id, scenario_id=scenario_id
    )


# ==============================
# NOTE: 시나리오 메타데이터


@private_simulation_router.get(
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


@private_simulation_router.put(
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
@private_simulation_router.post(
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
    redshift_db: Connection = Depends(get_redshift_connection),
    supabase_db: AsyncSession = Depends(aget_supabase_session),
):

    if not scenario_id:
        raise BadRequestException("Scenario ID is required")

    try:
        flight_sch = await sim_service.generate_flight_schedule(
            redshift_db,
            flight_schedule.date,
            flight_schedule.airport,
            flight_schedule.condition,
            scenario_id=scenario_id,
        )

        await sim_service.update_scenario_target_flight_schedule_date(
            supabase_db, scenario_id, flight_schedule.date
        )

        return flight_sch

    except SQLAlchemyError as e:
        logger.error(f"Database error while fetching scenario_id={scenario_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred while fetching the flight schedule.",
        )

    except Exception as e:
        logger.error(f"Unexpected error while fetching scenario_id={scenario_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while fetching the flight schedule.",
        )


@private_simulation_router.post(
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
    db: Connection = Depends(get_redshift_connection),
):
    return await sim_service.generate_passenger_schedule(
        db=db,
        flight_sch=passenger_schedule.flight_schedule,
        destribution_conditions=passenger_schedule.destribution_conditions,
        scenario_id=scenario_id,
    )


@private_simulation_router.post(
    "/processing-procedures",
    status_code=status.HTTP_200_OK,
    summary="06_SI_012",
    description="06_SI_012에서 설정한 운영세팅의 정보를 가져오는 엔드포인트",
)
@inject
async def fetch_processing_procedures(
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: Connection = Depends(get_redshift_connection),
):

    return await sim_service.fetch_processing_procedures()


@private_simulation_router.post(
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
    db: Connection = Depends(get_redshift_connection),
):
    return await sim_service.generate_facility_conn(
        scenario_id=scenario_id,
        processes=facility_conn.processes,
        target_date=facility_conn.flight_schedule.date,
    )


@private_simulation_router.post(
    "/facility-info/charts/line",
    status_code=status.HTTP_200_OK,
    summary="입력한 시설 용량을 기반으로 라인 차트 데이터를 생성하는 엔드포인트",
)
@inject
async def generate_set_opening_hours(
    facility_info: SetOpeningHoursBody,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
):

    return await sim_service.generate_set_opening_hours(facility_info=facility_info)


@private_simulation_router.post(
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
    db: Connection = Depends(get_redshift_connection),
):

    return await sim_service.generate_simulation_overview(
        db,
        run_simulation.flight_schedule,
        run_simulation.destribution_conditions,
        run_simulation.processes,
        run_simulation.components,
        scenario_id=scenario_id,
    )


@private_simulation_router.get(
    "/request-simulation/scenario-id/{scenario_id}",  # TODO: /scenario/{scenario_id}/request
    status_code=status.HTTP_200_OK,
)
@inject
async def fetch_simulation(
    scenario_id: str,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
):
    return await sim_service.get_simulation(scenario_id=scenario_id)


@private_simulation_router.post(
    "/request-simulation/scenario-id/{scenario_id}",  # TODO: /scenario/{scenario_id}/request
    status_code=status.HTTP_200_OK,
)
@inject
async def request_simulation(
    request: Request,
    scenario_id: str,
    payload: RunSimulationBody,
    background_tasks: BackgroundTasks,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    supabase_db: AsyncSession = Depends(aget_supabase_session),
):

    components, processes, flight_schedule = itemgetter(
        "components", "processes", "flight_schedule"
    )(payload.model_dump())

    return await sim_service.execute_simulation_by_scenario(
        db=supabase_db,
        user_id=request.state.user_id,
        schedule_date=flight_schedule.get("date"),
        scenario_id=scenario_id,
        components=components,
        processes=processes,
        background_tasks=background_tasks,
    )


@public_simulation_router.patch(
    "/end-simulation/scenario-id/{scenario_id}",  # TODO: /scenario/{scenario_id}/end
    status_code=status.HTTP_204_NO_CONTENT,
    summary="시나리오 시뮬레이션 종료 시간 업데이트",
    description="시나리오의 시뮬레이션 종료 시간을 기록합니다. 시뮬레이터 Lambda 함수가 시뮬레이션을 종료할 때 호출합니다.",
)
@inject
async def end_simulation(
    request: Request,
    scenario_id: str,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    supabase_db: AsyncSession = Depends(aget_supabase_session),
):
    if not scenario_id:
        raise BadRequestException("Scenario ID is required")

    await sim_service.update_simulation_status(
        db=supabase_db, user_id=request.state.user_id, scenario_id=scenario_id
    )


@public_simulation_router.patch(
    "/error-simulation/scenario-id/{scenario_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="시뮬레이션 오류 발생 시 오류 상태 업데이트",
    description="시뮬레이션 오류 발생 시 오류 상태를 업데이트합니다.",
)
@inject
async def update_simulation_error_status(
    request: Request,
    scenario_id: str,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    supabase_db: AsyncSession = Depends(aget_supabase_session),
):
    return await sim_service.update_simulation_error_status(
        db=supabase_db, user_id=request.state.user_id, scenario_id=scenario_id
    )
