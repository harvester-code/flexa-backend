from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import Connection

from src.airports.schema import ShowupBody, ChoiceMatrixBody
from src.airports.service import AirportService
from src.database import get_snowflake_session

airports_router = APIRouter(prefix="/airports")
airport_service = AirportService()


@airports_router.get("/general-declarations")
async def fetch_general_declarations(
    date: str,
    airport: str,
    flight_io: str = "departure",
    conn: Connection = Depends(get_snowflake_session),
):
    return await airport_service.fetch_general_declarations(
        date=date, airport=airport, flight_io=flight_io, conn=conn
    )


# FIXME: 매서드명 변경필요 확정적으로 무엇으로 부르면 좋을지?
@airports_router.post("/show-up")
async def show_up(item: ShowupBody):
    return airport_service.create_show_up(item)


# FIXME: 매서드명 변경필요 확정적으로 무엇으로 부르면 좋을지?
@airports_router.post("/airports/choice_matrix")
async def choice_matrix(item: ChoiceMatrixBody):
    return airport_service.create_choice_matrix(item)
