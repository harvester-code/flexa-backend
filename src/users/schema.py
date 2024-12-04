from pydantic import BaseModel
from typing import Union


class UserInfo(BaseModel):
    email: str
    first_name: Union[str, None] = None
    last_name: Union[str, None] = None
    password: Union[str, None] = None


class RequestAccess(BaseModel):
    user_id: str
    admin_email: str
    request_mg: str
    request_page: str
