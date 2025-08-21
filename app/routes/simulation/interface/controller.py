from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, HTTPException, Request, status
from loguru import logger
from sqlalchemy import Connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.libs.containers import Container
from app.libs.dependencies import verify_token
from app.libs.exceptions import BadRequestException
from app.routes.simulation.application.service import SimulationService
from app.routes.simulation.interface.schema import (
    FlightScheduleBody,
    PassengerScheduleBody,
    ScenarioDeactivateBody,
    ScenarioUpdateBody,
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
    "",
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
    "",
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
    "/{scenario_id}",
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
    logger.info(f"PUT /simulations/{scenario_id} called with data: {scenario}")
    
    if not scenario_id:
        raise BadRequestException("Scenario ID is required")

    await sim_service.update_scenario_information(
        db=db,
        scenario_id=scenario_id,
        name=scenario.name,
        terminal=scenario.terminal,
        airport=scenario.airport,
        memo=scenario.memo,
    )
    
    logger.info(f"Successfully updated scenario {scenario_id}")


@private_simulation_router.delete(
    "",
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
    "/{scenario_id}/master",
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


@private_simulation_router.post(
    "/{scenario_id}/metadata",
    status_code=status.HTTP_200_OK,
    summary="시나리오 메타데이터 S3 저장",
    description="시나리오 메타데이터를 S3에 직접 저장합니다",
)
@inject
async def save_scenario_metadata(
    scenario_id: str,
    metadata: dict,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
):
    if not scenario_id:
        raise BadRequestException("Scenario ID is required")

    if not metadata:
        raise BadRequestException("Metadata is required")

    return await sim_service.save_scenario_metadata(scenario_id, metadata)


@private_simulation_router.get(
    "/{scenario_id}/metadata",
    status_code=status.HTTP_200_OK,
    summary="시나리오 메타데이터 S3 로드",
    description="S3에서 시나리오 메타데이터를 불러옵니다",
)
@inject
async def load_scenario_metadata(
    scenario_id: str,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
):
    if not scenario_id:
        raise BadRequestException("Scenario ID is required")

    return await sim_service.load_scenario_metadata(scenario_id)


@private_simulation_router.post(
    "/{scenario_id}/flight-schedules",
    status_code=status.HTTP_200_OK,
    summary="시나리오별 항공편 스케줄 조회",
    description="지정된 날짜와 공항의 항공편 스케줄 데이터를 조회하고, 필터 조건에 따라 항공사별/터미널별/국내외별로 분류하여 차트 데이터를 생성합니다. 조회된 데이터는 S3에 저장되며, 시나리오의 대상 항공편 스케줄 날짜가 업데이트됩니다.",
)
@inject
async def fetch_scenario_flight_schedule(
    scenario_id: str,
    flight_schedule: FlightScheduleBody,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    redshift_db: Connection = Depends(get_redshift_connection),
    supabase_db: AsyncSession = Depends(aget_supabase_session),
):
    if not scenario_id:
        raise BadRequestException("Scenario ID is required")

    try:
        flight_sch = await sim_service.generate_scenario_flight_schedule(
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


# ==============================
# NOTE: 시뮬레이션 프로세스


@private_simulation_router.post(
    "/{scenario_id}/show-up-passenger",
    status_code=status.HTTP_200_OK,
    summary="승객 스케줄 생성",
    description="분배 조건을 기반으로 승객 스케줄 데이터를 생성합니다. 승객별 도착 시간 분포와 시간별 승객 흐름을 계산하여 시뮬레이션에 사용할 승객 데이터를 제공합니다.",
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
        destribution_conditions=passenger_schedule.destribution_conditions,
        scenario_id=scenario_id,
    )
