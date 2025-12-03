from fastapi import APIRouter, status

system_router = APIRouter()


@system_router.get(
    "/health",
    status_code=status.HTTP_200_OK,
    summary="Health Check",
    description="API 서버의 상태를 확인합니다.",
)
async def health_check():
    return {"status": "ok"}
