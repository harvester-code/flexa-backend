from fastapi import APIRouter

passenger_flow_router = APIRouter(prefix="/passenger-flows")


@passenger_flow_router.get("/maps")
async def fetch_passenger_flow_maps():
    return {"url": "http://localhost:8501/Passenger_Flow?embed=true"}
