import os

from fastapi import FastAPI, HTTPException, Request, status
from jose import JWTError, jwt
from starlette.responses import JSONResponse

# from src.main import app


def add_middlewares(app: FastAPI):
    """미들웨어를 FastAPI 앱에 추가하는 함수"""
    app.middleware("http")(jwt_decoder)


# =============================================
# TODO: refresh token과 refresh rotation 고려

SUPABASE_JWT_SECRET_KEY = os.getenv("SUPABASE_JWT_SECRET_KEY")
ALGORITHM = "HS256"
AUDIENCE = "authenticated"

EXCLUDED_PATHS = ["/docs", "/redoc", "/openapi.json", "/"]


async def jwt_decoder(request: Request, call_next):

    if request.method == "OPTIONS":
        return await call_next(request)

    if request.url.path in EXCLUDED_PATHS:
        return await call_next(request)

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Unauthorized",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        token = request.headers.get("Authorization")
        if not token or not token.startswith("Bearer "):
            raise credentials_exception

        token = token.split(" ")[1]

        payload = jwt.decode(
            token,
            SUPABASE_JWT_SECRET_KEY,
            algorithms=[ALGORITHM],
            audience=AUDIENCE,
        )

        user_id: str = payload.get("sub")
        if user_id is None:
            raise credentials_exception

        request.state.user_id = user_id

    except HTTPException as e:
        return JSONResponse(
            status_code=e.status_code,
            content={"detail": e.detail},
            headers=e.headers or {},
        )

    except JWTError:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"detail": "JWT DECODING ERROR"},
        )

    response = await call_next(request)
    return response


# =============================================
