from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from src.airports.service import AirportService
from src.database import get_snowflake_session

airports_router = APIRouter()


@airports_router.get("/airports/general-declarations")
async def fetch_general_declarations(session: Session = Depends(get_snowflake_session)):
    result = await run_in_threadpool(AirportService.fetch_general_declarations, session)
    return {"data": result}
