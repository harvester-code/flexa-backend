from pydantic import BaseModel


class UpdatePassword(BaseModel):
    user_id: str
    password: str
