from art import text2art
from fastapi import APIRouter, FastAPI
from fastapi.responses import PlainTextResponse
from starlette.middleware.cors import CORSMiddleware

from src.airports.router import airports_router
from src.auth.interface.controller import auth_router
from src.containers import Container
from src.simulation.router import simulation_router as past_simulation_router
from src.simulation.interface.controller import simulation_router

app = FastAPI()
app.container = Container()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

router = APIRouter()


@router.get("/", response_class=PlainTextResponse)
async def root():
    return text2art("FLEXA WAITFREE AIRPORT")


API_PREFIX = "/api/v1"

app.include_router(router)
# app.include_router(airports_router, prefix=API_PREFIX, tags=["Airports"])
# app.include_router(auth_router, prefix=API_PREFIX, tags=["Auth"])
# app.include_router(past_simulation_router, prefix=API_PREFIX, tags=["Simulations"])
app.include_router(simulation_router, prefix=API_PREFIX, tags=["Simulations"])
