from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from starlette.middleware.cors import CORSMiddleware

from app.libs.containers import Container
from app.libs.exceptions import add_exception_handlers
from app.libs.logging_config import setup_logging
from app.libs.middleware import AuthMiddleware
from app.libs.monitor_memory import setup_memory_monitor

from app.routes.auth.interface.controller import auth_router

from app.routes.home.interface.controller import home_router
from app.routes.simulation.interface.controller import (
    private_simulation_router,
    public_simulation_router,
)
from app.routes.system.interface.controller import system_router
from packages.doppler.client import get_secret
from packages.redshift.lifespan import lifespan

# 애플리케이션 상수
API_PREFIX = "/api/v1"

ALLOW_ORIGINS_MAP = {
    "local": ["*"],
    "dev": [
        "https://preview.flexa.expert",
        "http://localhost:3943",
    ],
    "prod": [
        "https://www.flexa.expert",
        "http://localhost:3943",
    ],
}

setup_logging()
setup_memory_monitor()

app = FastAPI(lifespan=lifespan)

app.container = Container()
app.container.wire()  # 🔧 의존성 주입 활성화!

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
app.include_router(private_simulation_router, prefix=API_PREFIX, tags=["Simulations"])
app.include_router(public_simulation_router, prefix=API_PREFIX, tags=["Simulations"])
app.include_router(home_router, prefix=API_PREFIX, tags=["Homes"])


app.include_router(system_router, prefix=API_PREFIX, tags=["System"])
