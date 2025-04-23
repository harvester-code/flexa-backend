from art import text2art
from fastapi import APIRouter, FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import PlainTextResponse
from loguru import logger
from mangum import Mangum
from starlette.middleware.cors import CORSMiddleware

from src.admin.interface.controller import admin_router
from src.containers import Container
from src.exceptions import add_exception_handlers
from src.facility.interface.controller import facility_router
from src.home.interface.controller import home_router
from src.middleware import add_middlewares
from src.passenger_flow.controller import passenger_flow_router
from src.simulation.interface.controller import simulation_router
from src.simulation.interface.websocket_controller import ws_router

app = FastAPI()
app.container = Container()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://flexa.datamarketingcampus.com",
    ],
    allow_credentials=True,
    # NOTE: 아래 코드는 오직 개발/테스트용
    # allow_origins=["*"],
    # allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)

add_middlewares(app)
add_exception_handlers(app)

API_PREFIX = "/api/v1"

router = APIRouter()


@router.get("/", response_class=PlainTextResponse)
async def root():
    return text2art("FLEXA WAITFREE AIRPORT")


@router.get("/health")
async def health():
    return {"status": "ok"}


# ================================
app.include_router(router, prefix=API_PREFIX)
app.include_router(simulation_router, prefix=API_PREFIX, tags=["Simulations"])
app.include_router(home_router, prefix=API_PREFIX, tags=["Homes"])
app.include_router(facility_router, prefix=API_PREFIX, tags=["Detailed-Facilities"])
app.include_router(passenger_flow_router, prefix=API_PREFIX, tags=["Passenger-Flow"])
app.include_router(admin_router, prefix=API_PREFIX, tags=["Admins"])

# ================================
app.include_router(ws_router, prefix=API_PREFIX, tags=["ws"])

# app.include_router(airports_router, prefix=API_PREFIX, tags=["Airports"])
# app.include_router(auth_router, prefix=API_PREFIX, tags=["Auth"])
# app.include_router(past_simulation_router, prefix=API_PREFIX, tags=["Simulations"])

try:
    handler = Mangum(app)
except Exception as e:
    logger.error(f"Error initializing Mangum: {e}")
