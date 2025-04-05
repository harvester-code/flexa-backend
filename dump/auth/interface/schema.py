from pydantic import BaseModel


class CreateCertificationBody(BaseModel):
    email: str


class CreateUserBody(BaseModel):
    email: str
    first_name: str
    last_name: str
    password: str


class CreateUserAccessRequestBody(BaseModel):
    user_email: str
    admin_email: str
    request_mg: str


class LoginUser(BaseModel):
    email: str
    password: str


class ResetPassword(BaseModel):
    user_id: str
    password: str
