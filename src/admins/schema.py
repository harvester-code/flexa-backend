from pydantic import BaseModel
from typing import List, Union, Generic, TypeVar
from datetime import datetime

T = TypeVar("T")


class MessageResponse(BaseModel, Generic[T]):
    status: int
    result: List[T]


class FullResponseModel(BaseModel, Generic[T]):
    message: MessageResponse[T]


class OutRequestUserInfo(BaseModel):
    user_email: str
    user_name: str
    created_at: datetime
    request_mg: str
    request_page: str


class RequestUser(BaseModel):
    user_email: str
    admin_id: Union[str, None] = None
    user_permissions: Union[list, None] = None


class OutUserManagement(BaseModel):
    user_name: str
    profile_url: Union[str, None] = None
    is_active: bool
    email: str
    permission: str
