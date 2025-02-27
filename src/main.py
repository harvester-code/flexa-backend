from art import text2art
from fastapi import APIRouter, FastAPI
from fastapi.responses import PlainTextResponse
from starlette.middleware.cors import CORSMiddleware

from src.containers import Container
from src.simulation.interface.controller import simulation_router
from src.middleware import add_middlewares

app = FastAPI()
app.container = Container()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

add_middlewares(app)

router = APIRouter()


@router.get("/", response_class=PlainTextResponse)
async def root():
    return text2art("FLEXA WAITFREE AIRPORT")


API_PREFIX = "/api/v1"

app.include_router(router)
app.include_router(simulation_router, prefix=API_PREFIX, tags=["Simulations"])

# app.include_router(airports_router, prefix=API_PREFIX, tags=["Airports"])
# app.include_router(auth_router, prefix=API_PREFIX, tags=["Auth"])
# app.include_router(past_simulation_router, prefix=API_PREFIX, tags=["Simulations"])
