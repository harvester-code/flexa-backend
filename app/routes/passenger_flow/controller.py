from fastapi import APIRouter

passenger_flow_router = APIRouter(prefix="/passenger-flows")


@passenger_flow_router.get(
    "/maps",
    summary="승객 흐름 지도 조회",
    description="승객 흐름 분석을 위한 Streamlit 기반 지도 서비스의 URL을 제공합니다. 실시간 승객 이동 패턴과 공간적 분포를 시각화한 대화형 지도에 접근할 수 있습니다.",
)
async def fetch_passenger_flow_maps():
    return {"url": "http://localhost:8501/Passenger_Flow?embed=true"}
