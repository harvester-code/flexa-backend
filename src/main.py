from art import text2art
from fastapi import APIRouter, FastAPI
from fastapi.responses import PlainTextResponse
from starlette.middleware.cors import CORSMiddleware

from src.admins.router import admins_router
from src.airports.router import airports_router
from src.auth.router import auth_router
from src.managements.router import managements_router
from src.profiles.router import profiles_router
from src.users.router import users_router

app = FastAPI()
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
app.include_router(users_router, prefix=API_PREFIX, tags=["Users"])
app.include_router(admins_router, prefix=API_PREFIX, tags=["Admins"])
app.include_router(profiles_router, prefix=API_PREFIX, tags=["Profiles"])
app.include_router(auth_router, prefix=API_PREFIX, tags=["Auth"])
app.include_router(managements_router, prefix=API_PREFIX, tags=["Managements"])
app.include_router(airports_router, prefix=API_PREFIX, tags=["Airports"])
