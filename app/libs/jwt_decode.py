import os

# from jose import jwt
import jwt

# AUDIENCE = "authenticated"
SECRET = os.getenv("SYSTEM_JWT_SECRET_KEY")
ALGORITHM = "HS256"


def decode_jwt(token) -> dict:

    payload = jwt.decode(token, SECRET, [ALGORITHM])

    return payload
