import os

from jose import jwt

SECRET = os.getenv("SYSTEM_JWT_SECRET_KEY")
ALGORITHM = "HS256"
AUDIENCE = "authenticated"


def decode_jwt(token) -> dict:

    payload = jwt.decode(
        token=token, key=SECRET, algorithms=[ALGORITHM], audience=AUDIENCE
    )
    return payload
