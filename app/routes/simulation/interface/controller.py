# Standard Library
from typing import List

# Third Party
from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, HTTPException, Request, status
from loguru import logger
from sqlalchemy import Connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

# Application
from app.libs.containers import Container
from app.libs.dependencies import verify_token
from app.libs.exceptions import BadRequestException
from app.routes.simulation.application.service import SimulationService
from app.routes.simulation.interface.schema import (
    FlightScheduleBody,
    PassengerScheduleBody,
    RunSimulationBody,
    ScenarioDeactivateBody,
    ScenarioUpdateBody,
    SimulationScenarioBody,
)
from packages.supabase.database import aget_supabase_session
from packages.redshift.client import get_redshift_connection

private_simulation_router = APIRouter(
    prefix="/simulations", dependencies=[Depends(verify_token)]
)

# NOTE: Lambda 함수에서 인증이 필요 없는 엔드포인트를 위한 라우터
public_simulation_router = APIRouter(
    prefix="/simulations",
)


"""
Simulation Controller - Clean Architecture

HTTP Status Code 정리:
- 200 OK: 요청 성공, 응답 데이터 포함
- 201 CREATED: 새 리소스 생성 성공  
- 204 NO_CONTENT: 요청 성공, 응답 본문 없음
- 400 BAD_REQUEST: 잘못된 요청
- 401 UNAUTHORIZED: 인증 실패
- 404 NOT_FOUND: 리소스 없음
- 500 INTERNAL_SERVER_ERROR: 서버 오류

API 순서:
1. 시나리오 관리 (기본 CRUD)
2. 항공편 스케줄 처리 (flight-schedules)  
3. 승객 스케줄 처리 (show-up-passenger)
4. 메타데이터 처리 (metadata save/load)
"""

# =====================================
# 1. 시나리오 관리 (기본 CRUD 기능)
# =====================================


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


# =====================================
# 2. 항공편 스케줄 처리 (Flight Schedule)
# =====================================


@private_simulation_router.post(
    "/{scenario_id}/flight-schedules",
    status_code=status.HTTP_200_OK,
    summary="시나리오별 항공편 스케줄 조회",
    description="지정된 날짜와 공항의 항공편 스케줄 데이터를 조회하고, 필터 조건에 따라 항공사별/터미널별/국내외별로 분류하여 차트 데이터를 생성합니다. 조회된 데이터는 S3에 저장되며, 시나리오의 대상 항공편 스케줄 날짜가 업데이트됩니다.",
)
@inject
async def fetch_scenario_flight_schedule(
    request: Request,
    scenario_id: str,
    flight_schedule: FlightScheduleBody,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    redshift_db: Connection = Depends(get_redshift_connection),
    supabase_db: AsyncSession = Depends(aget_supabase_session),
):
    if not scenario_id:
        raise BadRequestException("Scenario ID is required")

    # 🔒 시나리오 존재 여부 및 권한 검증
    scenario_exists = await sim_service.validate_scenario_exists(
        supabase_db, scenario_id, request.state.user_id
    )
    if not scenario_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Scenario '{scenario_id}' not found or you don't have permission to access it.",
        )

    try:
        # 디버그용 로그
        logger.info(f"🛩️ Flight Schedule Request - scenario_id: {scenario_id}")
        logger.info(
            f"📍 Request params: airport={flight_schedule.airport}, date={flight_schedule.date}, type={flight_schedule.type}"
        )
        logger.info(f"🔍 Conditions: {flight_schedule.conditions}")

        flight_sch = await sim_service.generate_scenario_flight_schedule(
            redshift_db,
            flight_schedule.date,
            flight_schedule.airport,
            flight_schedule.type,
            flight_schedule.conditions,
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
        logger.error(
            f"❌ Unexpected error while fetching scenario_id={scenario_id}: {e}"
        )
        logger.error(f"❌ Exception type: {type(e)}")
        import traceback

        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while fetching the flight schedule.",
        )


# =====================================
# 3. 승객 스케줄 처리 (Show-up Passenger)
# =====================================


@private_simulation_router.post(
    "/{scenario_id}/show-up-passenger",
    status_code=status.HTTP_200_OK,
    summary="승객 스케줄 생성",
    description="pax_simple.json 구조 기반으로 고도화된 승객 스케줄 데이터를 생성합니다. 승객별 도착 시간 분포, 인구통계, 시간별 승객 흐름을 계산하여 시뮬레이션에 사용할 승객 데이터를 제공합니다.",
)
@inject
async def generate_passenger_schedule(
    request: Request,
    scenario_id: str,
    passenger_schedule: PassengerScheduleBody,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    """승객 스케줄 생성 - pax_simple.json 구조 기반"""
    if not scenario_id:
        raise BadRequestException("Scenario ID is required")

    # 🔒 시나리오 존재 여부 및 권한 검증
    scenario_exists = await sim_service.validate_scenario_exists(
        db, scenario_id, request.state.user_id
    )
    if not scenario_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Scenario '{scenario_id}' not found or you don't have permission to access it.",
        )

    try:
        # PassengerScheduleBody를 dict로 변환
        config = passenger_schedule.model_dump()

        return await sim_service.generate_passenger_schedule(
            scenario_id=scenario_id,
            config=config,
        )
    except HTTPException:
        # HTTPException은 그대로 재발생
        raise
    except Exception as e:
        logger.error(f"Unexpected error in passenger schedule generation: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while generating passenger schedule.",
        )


# =====================================
# 4. 시뮬레이션 실행 (Run Simulation)
# =====================================


@private_simulation_router.post(
    "/{scenario_id}/run-simulation",
    status_code=status.HTTP_200_OK,
    summary="시뮬레이션 실행",
    description="승객 스케줄 데이터를 기반으로 공항 대기열 시뮬레이션을 실행합니다. SQS 메시지를 통해 Lambda 함수를 트리거하여 비동기로 시뮬레이션을 처리합니다.",
)
@inject
async def run_simulation(
    request: Request,
    scenario_id: str,
    simulation_request: RunSimulationBody,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    """시뮬레이션 실행 - SQS 메시지 전송을 통한 Lambda 트리거"""
    if not scenario_id:
        raise BadRequestException("Scenario ID is required")

    # 🔒 시나리오 존재 여부 및 권한 검증
    scenario_exists = await sim_service.validate_scenario_exists(
        db, scenario_id, request.state.user_id
    )
    if not scenario_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Scenario '{scenario_id}' not found or you don't have permission to access it.",
        )

    try:
        # 시뮬레이션 실행 요청 - SQS 메시지 전송
        result = await sim_service.run_simulation(
            scenario_id=scenario_id,
            process_flow=simulation_request.process_flow,
        )

        logger.info(f"🚀 시뮬레이션 실행 요청 완료: scenario_id={scenario_id}")

        return result

    except HTTPException:
        # HTTPException은 그대로 재발생
        raise
    except Exception as e:
        logger.error(f"Unexpected error in simulation execution: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while starting simulation.",
        )


# =====================================
# 5. 메타데이터 처리 (S3 Save/Load)
# =====================================


@private_simulation_router.post(
    "/{scenario_id}/metadata",
    status_code=status.HTTP_200_OK,
    summary="시나리오 메타데이터 S3 저장",
    description="시나리오 메타데이터를 S3에 직접 저장합니다",
)
@inject
async def save_scenario_metadata(
    request: Request,
    scenario_id: str,
    metadata: dict,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    if not scenario_id:
        raise BadRequestException("Scenario ID is required")

    if not metadata:
        raise BadRequestException("Metadata is required")

    # 🔒 시나리오 존재 여부 및 권한 검증
    scenario_exists = await sim_service.validate_scenario_exists(
        db, scenario_id, request.state.user_id
    )
    if not scenario_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Scenario '{scenario_id}' not found or you don't have permission to access it.",
        )

    return await sim_service.save_scenario_metadata(scenario_id, metadata)


@private_simulation_router.get(
    "/{scenario_id}/metadata",
    status_code=status.HTTP_200_OK,
    summary="시나리오 메타데이터 S3 로드",
    description="S3에서 시나리오 메타데이터를 불러옵니다",
)
@inject
async def load_scenario_metadata(
    request: Request,
    scenario_id: str,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    if not scenario_id:
        raise BadRequestException("Scenario ID is required")

    # 🔒 시나리오 존재 여부 및 권한 검증
    scenario_exists = await sim_service.validate_scenario_exists(
        db, scenario_id, request.state.user_id
    )
    if not scenario_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Scenario '{scenario_id}' not found or you don't have permission to access it.",
        )

    return await sim_service.load_scenario_metadata(scenario_id)
