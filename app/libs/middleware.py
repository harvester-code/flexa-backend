from typing import Callable

from fastapi import Request, status
from fastapi.responses import JSONResponse
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware

from app.libs.jwt_decode import decode_jwt
from packages.constants import API_PREFIX
from packages.supabase.auth import decode_supabase_token

PROTECTED_PATHS = [
    f"{API_PREFIX}/simulations",
    f"{API_PREFIX}/homes",
    f"{API_PREFIX}/facilities",
]

SYSTEM_PATHS = [
    f"{API_PREFIX}/simulations/end-simulation",
]


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        path = request.url.path

        # NOTE: CORS 사전 요청(Preflight Request)을 처리하기 위해 OPTIONS 메서드는 인증을 우회합니다.
        # 브라우저는 Authorization 헤더나 커스텀 헤더가 포함된 요청을 보내기 전에
        # 먼저 OPTIONS 메서드로 사전 요청을 보냅니다.
        # 이때 서버가 401/403을 반환하면 CORS 오류로 간주되어 실제 요청까지 도달하지 못합니다.
        # 따라서 OPTIONS 요청은 인증 없이 통과시킵니다.
        if request.method == "OPTIONS":
            return await call_next(request)

        # ==============
        # DEBUG
        logger.info(f"🔍 Raw request path: '{path}'")
        logger.info(f"🔍 Path bytes: {path.encode('utf-8')}")
        # ==============

        if any(path.startswith(protected_path) for protected_path in PROTECTED_PATHS):
            auth_header = request.headers.get("Authorization")

            if not auth_header or not auth_header.startswith("Bearer "):
                return JSONResponse(
                    {"detail": "Authorization header missing or invalid"},
                    status_code=status.HTTP_401_UNAUTHORIZED,
                )

            token = auth_header.split(" ")[1]

            # NOTE: 시스템(ex. Lambda) 환경에서는 다른 jwt 인증을 사용합니다.
            try:

                # ==============
                # DEBUG
                logger.info(f"🔍 SYSTEM_PATHS: {SYSTEM_PATHS}")
                for i, system_path in enumerate(SYSTEM_PATHS):
                    logger.info(
                        f"🔍 SYSTEM_PATH[{i}]: '{system_path}' (length: {len(system_path)})"
                    )
                    logger.info(f"🔍 Starts with check: {path.startswith(system_path)}")

                if any(path.startswith(system_path) for system_path in SYSTEM_PATHS):
                    logger.info("Inside lambda decode")
                    payload = decode_jwt(token=token)
                    logger.info("get payload")
                    logger.info(f"sub: {bool(payload.get('sub'))}")
                    logger.info(payload.get("iss"))
                    request.state.user_id = payload.get("sub")
                    logger.info("state fin")

                else:
                    logger.info("Inside supabase decode")
                    user = decode_supabase_token(token)
                    request.state.user_id = user.id
                # ==============

            except Exception as e:
                return JSONResponse(
                    {"detail": f"Middleware Error: {str(e)}"},
                    status_code=status.HTTP_401_UNAUTHORIZED,
                )

        return await call_next(request)
