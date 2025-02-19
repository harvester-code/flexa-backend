import pendulum
from fastapi import Depends, HTTPException, status, FastAPI
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
import os
from datetime import datetime, timedelta
import re


class TimeStamp:

    tz = pendulum.timezone("Asia/Seoul")

    def time_now(self):
        return pendulum.now(tz=self.tz).replace(tzinfo=None)


# =============================================
SUPABASE_JWT_SECRET_KEY = os.getenv("SUPABASE_JWT_SECRET_KEY")
# print(SUPABASE_JWT_SECRET_KEY)
ALGORITHM = "RS256"

# 토큰이 없을경우 401에러와 함께 "Not authenticated" 반환
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")


def verify_jwt(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(
            token,
            SUPABASE_JWT_SECRET_KEY,
            algorithms=[ALGORITHM],
            audience="authenticated",
        )
        # print(payload)
        user_id: str = payload.get("sub")
        if user_id is None:
            raise credentials_exception

    except JWTError as e:
        # print(f"Token verification failed: {str(e)}")  # 디버깅용
        raise credentials_exception

    return user_id
