from fastapi import APIRouter, status
from app.libs.response import SuccessResponse

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
