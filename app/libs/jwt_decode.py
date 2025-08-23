import jwt
from fastapi import HTTPException, status
from jwt.exceptions import ExpiredSignatureError, InvalidTokenError

from packages.doppler.client import get_secret

SECRET = get_secret("SYSTEM_JWT_SECRET_KEY")
ALGORITHM = "HS256"


def decode_jwt(token) -> dict:
    if not SECRET:
        raise ValueError("SYSTEM_JWT_SECRET_KEY environment variable is not set")

    try:
        payload = jwt.decode(token, SECRET, algorithms=[ALGORITHM])
        return payload
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Token has expired"
        )
    except InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )
