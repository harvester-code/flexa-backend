import os

from art import text2art
from fastapi import APIRouter, FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import PlainTextResponse
from loguru import logger
from mangum import Mangum
from starlette.middleware.cors import CORSMiddleware

from app.libs.containers import Container
from app.libs.exceptions import add_exception_handlers
from app.libs.middleware import add_middlewares
from app.routes.admin.interface.controller import admin_router
from app.routes.auth.interface.controller import auth_router
from app.routes.facility.interface.controller import facility_router
from app.routes.home.interface.controller import home_router
from app.routes.passenger_flow.controller import passenger_flow_router
from app.routes.simulation.interface.controller import simulation_router
from packages.constants import ALLOW_ORIGINS_MAP

app = FastAPI()
app.container = Container()
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS_MAP.get(os.getenv("ENVIRONMENT")),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)

add_middlewares(app)
add_exception_handlers(app)

# ================================================================
router = APIRouter()

@router.get("/", response_class=PlainTextResponse)
async def root():
    return text2art("FLEXA WAITFREE AIRPORT")
# ================================================================
API_PREFIX = "/api/v1"

app.include_router(router, prefix=API_PREFIX)
app.include_router(auth_router, prefix=API_PREFIX, tags=["Authentication"])
app.include_router(simulation_router, prefix=API_PREFIX, tags=["Simulations"])
app.include_router(home_router, prefix=API_PREFIX, tags=["Homes"])
app.include_router(facility_router, prefix=API_PREFIX, tags=["Detailed-Facilities"])
app.include_router(passenger_flow_router, prefix=API_PREFIX, tags=["Passenger-Flow"])
app.include_router(admin_router, prefix=API_PREFIX, tags=["Admins"])

try:
    handler = Mangum(app)
except Exception as e:
    logger.error(f"Error initializing Mangum: {e}")
