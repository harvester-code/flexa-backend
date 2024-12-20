from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import Connection

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


# FIXME: 임시
class Item(BaseModel):
    inputs: Any


@airports_router.post("/show-up")
async def show_up(inputs: Item):
    return airport_service.show_up(inputs)
