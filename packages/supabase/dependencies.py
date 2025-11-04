"""
Supabase 관련 FastAPI 의존성들

이 모듈은 Supabase와 관련된 FastAPI 의존성들을 제공합니다.
주로 권한 검증 및 데이터 접근 제어 관련 의존성들을 포함합니다.
"""

from typing import Annotated

from dependency_injector.wiring import Provide, inject
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from app.libs.containers import Container
from app.routes.simulation.application.service import SimulationService
from packages.supabase.auth import decode_supabase_token
from packages.supabase.database import aget_supabase_session

# FastAPI security scheme for Bearer token
security = HTTPBearer()


async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    Supabase 토큰 검증 FastAPI 의존성

    Bearer 토큰을 검증하고 사용자 정보를 반환합니다.
    주로 FastAPI docs의 "Authorize" 버튼 기능을 위해 사용됩니다.

    Args:
        credentials: HTTP Authorization 헤더의 Bearer 토큰

    Returns:
        User: 검증된 Supabase 사용자 객체

    Raises:
        HTTPException: 토큰이 유효하지 않은 경우
    """

    return decode_supabase_token(credentials.credentials)


@inject  # 🔧 누락된 데코레이터 추가!
async def verify_scenario_ownership(
    scenario_id: str,
    request: Request,
    sim_service: SimulationService = Depends(Provide[Container.simulation_service]),
    db: AsyncSession = Depends(aget_supabase_session),
) -> str:
    """
    시나리오 소유권 검증 FastAPI 의존성

    현재 사용자가 해당 시나리오의 소유자인지 확인합니다.
    - 시나리오 존재 여부 확인
    - 사용자 소유권 확인

    Args:
        scenario_id: 검증할 시나리오 ID
        request: FastAPI 요청 객체 (user_id 포함)
        sim_service: 시뮬레이션 서비스 (DI)
        db: Supabase 데이터베이스 세션 (DI)

    Returns:
        str: 검증된 scenario_id

    Raises:
        HTTPException: 시나리오가 존재하지 않거나 권한이 없는 경우
    """

    if not scenario_id or not scenario_id.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Scenario ID is required and cannot be empty",
        )

    try:
        # 시나리오 존재 여부 및 사용자 소유권 검증
        scenario_exists = await sim_service.validate_scenario_exists(
            db, scenario_id, request.state.user_id
        )

        if not scenario_exists:
            logger.warning(
                f"Scenario access denied - scenario_id: {scenario_id}, "
                f"user_id: {request.state.user_id}"
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Scenario '{scenario_id}' not found or you don't have permission to access it.",
            )

        logger.debug(
            f"Scenario access granted - scenario_id: {scenario_id}, "
            f"user_id: {request.state.user_id}"
        )

        return scenario_id

    except HTTPException:
        # HTTPException은 그대로 재발생
        raise
    except Exception as e:
        logger.error(
            f"Unexpected error during scenario ownership verification: {str(e)} "
            f"- scenario_id: {scenario_id}, user_id: {request.state.user_id}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to verify scenario ownership",
        )


