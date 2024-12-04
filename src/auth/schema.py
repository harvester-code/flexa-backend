from pydantic import BaseModel
from typing import Dict, Any


class UpdatePassword(BaseModel):
    user_id: str
    password: str
