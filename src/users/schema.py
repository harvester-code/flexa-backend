from typing import Union

from pydantic import BaseModel


class Certification(BaseModel):
    email: str


class UserCreate(BaseModel):
    email: str
    first_name: str
    last_name: str
    password: str


class RequestAccess(BaseModel):
    user_id: str
    admin_email: str
    request_mg: str
