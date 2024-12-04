from pydantic import BaseModel
from typing import List, Union, Generic, TypeVar
from datetime import datetime

T = TypeVar("T")


class MessageResponse(BaseModel, Generic[T]):
    status: int
    result: List[T]


class FullResponseModel(BaseModel, Generic[T]):
    message: MessageResponse[T]


class OutSimulationList(BaseModel):
    id: str
    user_id: str
    type: str
    simulation_name: str
    simulation_url: Union[str, None] = None
    size: Union[float, None] = None
    terminal: Union[str, None] = None
    editor: Union[str, None] = None
    memo: Union[str, None] = None
    simulated_at: Union[datetime, None] = None
    updated_at: datetime


class OutFilterList(BaseModel):
    id: str
    user_id: str
    type: str
    filter_name: str
    filter_json: Union[dict, None] = None
    size: Union[float, None] = None
    page: Union[str, None] = None
    category: Union[str, None] = None
    terminal: Union[str, None] = None
    memo: Union[str, None] = None
    created_at: datetime


class UpdateFileName(BaseModel):
    category: str
    old_name: str
    new_name: str
