from pydantic import BaseModel, Field
from typing import List, Union, Generic, TypeVar, Optional
from datetime import datetime

T = TypeVar("T")


class MessageResponse(BaseModel, Generic[T]):
    status: int
    result: List[T]


class FullResponseModel(BaseModel, Generic[T]):
    message: MessageResponse[T]


class InUserInfo(BaseModel):
    user_id: str
    first_name: str
    last_name: str
    profile_image_url: Optional[str] = Field(default=None)
    position: Union[str, None] = None
    bio: Union[str, None] = None


class OutUserInfo(BaseModel):
    first_name: str
    last_name: str
    email: str
    profile_image_url: Optional[str] = Field(default=None)
    position: Union[str, None] = None
    bio: Union[str, None] = None


class OutUserHistory(BaseModel):
    user_agent: str
    ip_address: str
    updated_at: datetime
