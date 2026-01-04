# Standard Library
from typing import List

# Third Party
from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from loguru import logger
from sqlalchemy import Connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

# Application
from app.libs.containers import Container
from packages.supabase.dependencies import verify_token
from app.libs.exceptions import BadRequestException
from app.routes.simulation.application.service import SimulationService
from app.routes.simulation.interface.schema import (
    FlightScheduleBody,
    FlightFiltersResponse,
    PassengerScheduleBody,
    RunSimulationBody,
    ScenarioDeactivateBody,
    ScenarioCopyRequest,
    ScenarioCopyResponse,
    ScenarioUpdateBody,
    SimulationScenarioBody,
)
from packages.supabase.database import aget_supabase_session
from packages.supabase.dependencies import verify_scenario_ownership
from packages.postgresql.client import get_postgresql_connection

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
    scenario: ScenarioUpdateBody,
    scenario_id: str = Depends(
        verify_scenario_ownership
    ),  # 🔧 @inject 추가된 의존성 재테스트!
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    logger.info(f"PUT /simulations/{scenario_id} called with data: {scenario}")

    # ✅ 권한 검증은 의존성에서 이미 처리됨, 바로 비즈니스 로직 실행
    await sim_service.update_scenario_information(
        db=db,
        scenario_id=scenario_id,
        name=scenario.name,
        terminal=scenario.terminal,
        airport=scenario.airport,
        memo=scenario.memo,
    )

    logger.info(f"Successfully updated scenario {scenario_id}")


@private_simulation_router.post(
    "/{scenario_id}/copy",
    status_code=status.HTTP_201_CREATED,
    response_model=ScenarioCopyResponse,
    summary="시나리오 복사",
    description="기존 시나리오를 복사하여 새로운 시나리오를 생성합니다. Supabase 데이터와 S3 데이터를 모두 복사합니다.",
)
@inject
async def copy_scenario(
    request: Request,
    copy_request: ScenarioCopyRequest = ScenarioCopyRequest(),  # 복사 요청 body (선택사항)
    scenario_id: str = Depends(verify_scenario_ownership),  # 원본 시나리오 권한 검증
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    """
    시나리오 복사

    1. 원본 시나리오 데이터 조회 (Supabase)
    2. 새 시나리오 생성 (새 UUID)
    3. S3 데이터 복사 (원본 폴더 → 새 폴더)
    4. 새로 생성된 시나리오 정보 반환
    """
    logger.info(f"POST /simulations/{scenario_id}/copy called")

    try:
        # ✅ 권한 검증은 의존성에서 이미 처리됨, 바로 비즈니스 로직 실행
        new_scenario = await sim_service.copy_scenario_information(
            db=db,
            source_scenario_id=scenario_id,
            user_id=request.state.user_id,
            new_name=copy_request.name,  # 프론트엔드에서 전달한 이름 (선택사항)
        )

        logger.info(f"✅ Successfully copied scenario {scenario_id} → {new_scenario['scenario_id']}")

        return ScenarioCopyResponse(
            scenario_id=new_scenario["scenario_id"],
            name=new_scenario["name"],
            terminal=new_scenario["terminal"],
            airport=new_scenario["airport"],
            memo=new_scenario["memo"],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error copying scenario {scenario_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while copying the scenario.",
        )


@private_simulation_router.delete(
    "",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="시나리오 영구 삭제",
    description="시나리오를 영구적으로 삭제합니다 (Supabase + S3 데이터 모두 삭제)",
)
@inject
async def delete_scenarios(
    request: Request,
    scenario_ids: ScenarioDeactivateBody,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    # ✅ Service layer에서 bulk 권한 검증과 영구 삭제를 일괄 처리
    await sim_service.delete_scenarios_permanently(
        db=db, scenario_ids=scenario_ids.scenario_ids, user_id=request.state.user_id
    )


# =====================================
# 2. 항공편 필터링 메타데이터 (Flight Filters)
# =====================================


@private_simulation_router.get(
    "/{scenario_id}/flight-filters",
    status_code=status.HTTP_200_OK,
    response_model=FlightFiltersResponse,
    summary="항공편 필터링 메타데이터 조회",
    description="시나리오별 항공편 필터링 옵션을 제공합니다. Departure/Arrival 모드별로 사용 가능한 필터들(터미널, 지역, 항공사 등)과 각 필터별 항공편 수를 반환합니다.",
)
@inject
async def get_flight_filters(
    scenario_id: str = Depends(verify_scenario_ownership),  # ✅ 권한 검증
    airport: str = Query(..., description="공항 IATA 코드 (예: ICN)"),
    date: str = Query(..., description="대상 날짜 (YYYY-MM-DD)"),
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    postgresql_db: Connection = Depends(get_postgresql_connection),
    db: AsyncSession = Depends(aget_supabase_session),
):
    """
    항공편 필터링 메타데이터 조회

    사용자가 항공편을 필터링할 수 있는 모든 옵션을 제공합니다:
    - departure: ICN 출발편 필터들 (출발터미널, 도착지역/국가 등)
    - arrival: ICN 도착편 필터들 (도착터미널, 출발지역/국가 등)

    각 필터별로 항공편 수와 실제 편명 리스트도 함께 제공됩니다.
    """
    try:
        logger.info(f"🔍 Flight filters request - scenario_id: {scenario_id}")
        logger.info(f"📍 Parameters: airport={airport}, date={date}")

        # ✅ 권한 검증은 의존성에서 이미 처리됨, 바로 비즈니스 로직 실행
        filters_metadata = await sim_service.get_flight_filters_metadata(
            postgresql_db=postgresql_db, scenario_id=scenario_id, airport=airport, date=date
        )

        logger.info(
            f"✅ Flight filters generated successfully for scenario {scenario_id}"
        )
        return filters_metadata

    except HTTPException:
        # HTTPException은 그대로 재발생
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error in flight filters: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while generating flight filters.",
        )


# =====================================
# 3. 항공편 스케줄 처리 (Flight Schedule)
# =====================================


@private_simulation_router.post(
    "/{scenario_id}/flight-schedules",
    status_code=status.HTTP_200_OK,
    summary="시나리오별 항공편 스케줄 조회",
    description="지정된 날짜와 공항의 항공편 스케줄 데이터를 조회하고, 필터 조건에 따라 항공사별/터미널별/국내외별로 분류하여 차트 데이터를 생성합니다. 조회된 데이터는 S3에 저장되며, 시나리오의 대상 항공편 스케줄 날짜가 업데이트됩니다.",
)
@inject
async def fetch_scenario_flight_schedule(
    flight_schedule: FlightScheduleBody,
    scenario_id: str = Depends(verify_scenario_ownership),  # ✅ 의존성 방식으로 통일
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    postgresql_db: Connection = Depends(get_postgresql_connection),
    supabase_db: AsyncSession = Depends(aget_supabase_session),
):
    # ✅ 권한 검증은 의존성에서 이미 처리됨, 바로 비즈니스 로직 실행
    try:
        # 디버그용 로그
        logger.info(f"🛩️ Flight Schedule Request - scenario_id: {scenario_id}")
        logger.info(
            f"📍 Request params: airport={flight_schedule.airport}, date={flight_schedule.date}, type={flight_schedule.type}"
        )
        logger.info(f"🔍 Conditions: {flight_schedule.conditions}")

        flight_sch = await sim_service.generate_scenario_flight_schedule(
            postgresql_db,
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
    description="pax_simple.json 구조 기반으로 고도화된 승객 스케줄 데이터를 생성합니다. 승객별 도착 시간 분포, 인구통계 (nationality/profile은 정수로 받아 확률로 변환), 시간별 승객 흐름을 계산하여 시뮬레이션에 사용할 승객 데이터를 제공합니다.",
)
@inject
async def generate_passenger_schedule(
    passenger_schedule: PassengerScheduleBody,
    scenario_id: str = Depends(verify_scenario_ownership),  # ✅ 의존성 방식으로 통일
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    """승객 스케줄 생성 - pax_simple.json 구조 기반"""
    # ✅ 권한 검증은 의존성에서 이미 처리됨, 바로 비즈니스 로직 실행
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
    simulation_request: RunSimulationBody,
    scenario_id: str = Depends(verify_scenario_ownership),  # ✅ 의존성 방식으로 통일
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    """시뮬레이션 실행 - SQS 메시지 전송을 통한 Lambda 트리거"""
    # ✅ 권한 검증은 의존성에서 이미 처리됨, 바로 비즈니스 로직 실행
    try:
        # setting 데이터 처리 (scenario_id는 PATH 파라미터에서만 사용)
        setting = simulation_request.setting.copy()

        logger.info(f"🎯 시뮬레이션 설정: {setting}")

        # 시뮬레이션 실행 요청 - SQS 메시지 전송 (setting 포함)
        result = await sim_service.run_simulation(
            scenario_id=scenario_id,
            setting=setting,
            process_flow=simulation_request.process_flow,
            db=db,
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
    summary="시나리오 메타데이터 S3 저장 및 Supabase 업데이트",
    description="시나리오 메타데이터를 S3에 저장하고 Supabase의 metadata_updated_at도 업데이트합니다",
)
@inject
async def save_scenario_metadata(
    metadata: dict,
    scenario_id: str = Depends(verify_scenario_ownership),  # ✅ 의존성 방식으로 통일
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    if not metadata:
        raise BadRequestException("Metadata is required")

    # ✅ 권한 검증은 의존성에서 이미 처리됨, 바로 비즈니스 로직 실행
    return await sim_service.save_scenario_metadata(scenario_id, metadata, db)


@private_simulation_router.get(
    "/{scenario_id}/metadata",
    status_code=status.HTTP_200_OK,
    summary="시나리오 메타데이터 S3 로드",
    description="S3에서 시나리오 메타데이터를 불러옵니다",
)
@inject
async def load_scenario_metadata(
    scenario_id: str = Depends(verify_scenario_ownership),  # ✅ 의존성 방식으로 통일
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    # ✅ 권한 검증은 의존성에서 이미 처리됨, 바로 비즈니스 로직 실행
    return await sim_service.load_scenario_metadata(scenario_id)


@private_simulation_router.delete(
    "/{scenario_id}/metadata",
    status_code=status.HTTP_200_OK,
    summary="시나리오 메타데이터 S3 삭제",
    description="S3에서 시나리오 메타데이터를 삭제합니다",
)
@inject
async def delete_scenario_metadata(
    scenario_id: str = Depends(verify_scenario_ownership),  # ✅ 의존성 방식으로 통일
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    # ✅ 권한 검증은 의존성에서 이미 처리됨, 바로 비즈니스 로직 실행
    return await sim_service.delete_scenario_metadata(scenario_id)


