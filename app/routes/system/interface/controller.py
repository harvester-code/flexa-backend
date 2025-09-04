import asyncio
from fastapi import APIRouter, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.libs.response import SuccessResponse
from packages.supabase.database import (
    aget_supabase_session,
    check_db_health,
    get_pool_status,
    ENVIRONMENT
)

system_router = APIRouter()


@system_router.get(
    "/health",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="애플리케이션 헬스체크 엔드포인트",
    description="API 서버의 상태를 확인하는 헬스체크 엔드포인트입니다.",
    tags=["System"],
)
async def health_check():
    """
    헬스체크 엔드포인트

    애플리케이션이 정상적으로 동작하는지 확인합니다.
    """
    return SuccessResponse(
        status_code=status.HTTP_200_OK,
    )


@system_router.get(
    "/health/detailed",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="상세 시스템 상태 확인",
    description="DB 연결 상태 및 연결 풀 모니터링 - 개발 환경에서 장시간 사용시 모니터링용",
    tags=["System"],
)
async def detailed_health_check(db: AsyncSession = Depends(aget_supabase_session)):
    """상세 시스템 상태 확인 - 개발 환경에서 DB 연결 상태 모니터링용"""
    
    # DB 연결 상태 확인
    db_healthy = await check_db_health()
    
    # 연결 풀 상태 확인
    pool_status = get_pool_status()
    
    # 전체 시스템 상태
    overall_status = "healthy" if db_healthy else "unhealthy"
    
    health_data = {
        "status": overall_status,
        "service": "flexa-waitfree-api",
        "environment": ENVIRONMENT,
        "database": {
            "status": "connected" if db_healthy else "disconnected",
            "pool": pool_status
        }
    }
    
    return SuccessResponse(
        status_code=status.HTTP_200_OK,
        data=health_data
    )


@system_router.get(
    "/health/db-pool", 
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="DB 연결 풀 실시간 상태",
    description="개발시 DB 연결 풀 사용량 모니터링 - 장시간 개발시 연결 상태 확인용",
    tags=["System"],
)
async def db_pool_status_check():
    """DB 연결 풀 실시간 상태 - 개발시 모니터링용"""
    
    pool_info = get_pool_status()
    
    # 연결 풀 사용률 계산
    total_connections = pool_info["pool_size"] + pool_info["overflow"]
    used_connections = pool_info["checked_out"]
    usage_percentage = (used_connections / total_connections) * 100 if total_connections > 0 else 0
    
    pool_data = {
        **pool_info,
        "total_available": total_connections,
        "usage_percentage": round(usage_percentage, 2),
        "environment": ENVIRONMENT,
        "recommendations": []
    }
    
    # 사용률에 따른 권장사항
    if usage_percentage > 80:
        pool_data["recommendations"].append("High pool usage - consider increasing pool_size")
    elif usage_percentage > 90:
        pool_data["recommendations"].append("Critical pool usage - immediate action needed")
    
    return SuccessResponse(
        status_code=status.HTTP_200_OK,
        data=pool_data
    )