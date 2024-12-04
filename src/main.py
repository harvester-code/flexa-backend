# from typing import Union
# from pydantic import BaseModel
from fastapi import APIRouter, FastAPI
from starlette.middleware.cors import CORSMiddleware

from src.users.router import users_router
from src.admins.router import admins_router
from src.profiles.router import profiles_router
from src.auth.router import auth_router
from src.managements.router import managements_router

router = APIRouter()
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@router.get("/")
async def root():
    return {"message": "Hello"}


app.include_router(router)
app.include_router(users_router, prefix="/api/v1", tags=["Users"])
app.include_router(admins_router, prefix="/api/v1", tags=["Admins"])
app.include_router(profiles_router, prefix="/api/v1", tags=["Profiles"])
app.include_router(auth_router, prefix="/api/v1", tags=["Auth"])
app.include_router(managements_router, prefix="/api/v1", tags=["Managements"])
