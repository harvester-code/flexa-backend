from pydantic import BaseModel
from typing import Any


class BaseResponse(BaseModel):
    is_success: bool
    status_code: int
    message: str
    data: Any = None


class SuccessResponse(BaseResponse):
    is_success: bool = True
    message: str = "Response Success!"


class ErrorResponse(BaseResponse):
    is_success: bool = False
    message: str = "Response Error!"
