import os

from fastapi import FastAPI, HTTPException, Request, WebSocket, status
from jose import JWTError, jwt
from loguru import logger
from starlette.responses import JSONResponse


def add_middlewares(app: FastAPI):
    """미들웨어를 FastAPI 앱에 추가하는 함수"""
    app.middleware("http")(jwt_decoder)


# =============================================
# TODO: refresh token과 refresh rotation 고려

SUPABASE_JWT_SECRET_KEY = os.getenv("SUPABASE_JWT_SECRET_KEY")
ALGORITHM = "HS256"
AUDIENCE = "authenticated"

# FIXME: 차후 개발이 완성되면 모든 api는 해당 jwt 인증을 거치도록 설정
EXCLUDED_PATHS = ["/docs", "/redoc", "/openapi.json", "/"]
JWT_DECODER_PATH = [
    "/api/v1/simulations/scenario",
    "/api/v1/simulations/scenario/deactivate",
    "/api/v1/simulations/scenario/deactivate/multiple",
    "/api/v1/simulations/scenario/duplicate",
    "/api/v1/simulations/scenario/master",
    "/api/v1/simulations/scenario/metadata",
    "/api/v1/simulations/kpi-chart",
    "/api/v1/simulations/total-chart",
]


# =============================================
# NOTE: http request


async def jwt_decoder(request: Request, call_next):
    logger.info(
        {
            "method": request.method,
            "url": str(request.url),
            "headers": dict(request.headers),
            "query_params": dict(request.query_params),
            "path_params": request.path_params,
        }
    )

    if request.method == "OPTIONS":
        return await call_next(request)

    if request.url.path in EXCLUDED_PATHS:
        return await call_next(request)

    # FIXME: 나중에는 정리필요
    # if request.url.path not in JWT_DECODER_PATH:
    #     return await call_next(request)

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
# NOTE: websocket


async def websocket_jwt_decoder(websocket: WebSocket):

    token = websocket.headers.get("sec-websocket-protocol")
    await websocket.accept(subprotocol=token)

    if not token:
        await websocket.close(
            code=status.WS_1008_POLICY_VIOLATION, reason="Please Sec-WebSocket-Protocol"
        )
        raise

    # token = token.split(" ")[1]

    try:
        payload = jwt.decode(
            token,
            SUPABASE_JWT_SECRET_KEY,
            algorithms=[ALGORITHM],
            audience=AUDIENCE,
        )
        user_id = payload.get("sub")
        if user_id is None:
            await websocket.close(
                code=status.WS_1008_POLICY_VIOLATION,
                reason="Check user_id in Access Token",
            )
            raise

        websocket.state.user_id = user_id

    except JWTError:
        await websocket.close(
            code=status.WS_1008_POLICY_VIOLATION, reason="Check Access Token"
        )
        raise


# =============================================
