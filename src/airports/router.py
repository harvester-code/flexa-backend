from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from src.airports.service import AirportService
from src.database import get_snowflake_session

from src.airports.model import ShowUpFilterList


airports_router = APIRouter()
airport_service = AirportService()


@airports_router.get("/airports/general-declarations")
async def fetch_general_declarations(
    date: str,
    airport: str,
    flight_io: str = "departure",
    session: Session = Depends(get_snowflake_session),
):
    # TODO: 아래 방법이 맞는지 확인하기
    data = await run_in_threadpool(
        AirportService.fetch_general_declarations, date, airport, flight_io, session
    )
    return data


@airports_router.post("/airports/show-up")
async def show_up(inputs: ShowUpFilterList):

    return airport_service.show_up(inputs)
