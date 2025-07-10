import threading

from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from starlette.middleware.cors import CORSMiddleware

from app.libs.containers import Container
from app.libs.exceptions import add_exception_handlers
from app.libs.middleware import AuthMiddleware
from app.libs.monitor_memory import monitor_memory
from app.routes.admin.interface.controller import admin_router
from app.routes.auth.interface.controller import auth_router
from app.routes.facility.interface.controller import facility_router
from app.routes.home.interface.controller import home_router
from app.routes.passenger_flow.controller import passenger_flow_router
from app.routes.simulation.interface.controller import simulation_router
from packages.constants import ALLOW_ORIGINS_MAP, API_PREFIX
from packages.secrets import get_secret

if get_secret("ENVIRONMENT") == "dev":
    threading.Thread(target=monitor_memory, daemon=True).start()

app = FastAPI()

app.container = Container()

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS_MAP.get(get_secret("ENVIRONMENT")),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)
app.add_middleware(AuthMiddleware)

add_exception_handlers(app)

# ================================================================
app.include_router(auth_router, prefix=API_PREFIX, tags=["Authentication"])
app.include_router(simulation_router, prefix=API_PREFIX, tags=["Simulations"])
app.include_router(home_router, prefix=API_PREFIX, tags=["Homes"])
app.include_router(facility_router, prefix=API_PREFIX, tags=["Detailed-Facilities"])
app.include_router(passenger_flow_router, prefix=API_PREFIX, tags=["Passenger-Flow"])
app.include_router(admin_router, prefix=API_PREFIX, tags=["Admins"])
