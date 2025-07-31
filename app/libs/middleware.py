import time
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

# HACK: 현재는 PROTECTED_PATHS와 SYSTEM_PATHS가 동일한 경로를 포함하고 있습니다.
SYSTEM_PATHS = [
    f"{API_PREFIX}/simulations/end-simulation",
    f"{API_PREFIX}/simulations/scenario",
]


class AuthMiddleware(BaseHTTPMiddleware):
    def _extract_token_from_header(self, request: Request) -> str:
        """Extracts the Bearer token from the Authorization header.

        Args:
            request (Request): The incoming request object.

        Raises:
            ValueError: If the Authorization header is missing or invalid.

        Returns:
            str: The extracted Bearer token.
        """

        auth_header = request.headers.get("Authorization")

        if not auth_header or not auth_header.startswith("Bearer "):
            raise ValueError("Authorization header missing or invalid")

        return auth_header.split(" ")[1]

    def _handle_auth_error(self) -> JSONResponse:
        """Handles authentication errors by returning a JSON response.

        Returns:
            JSONResponse: The JSON response containing the error details.
        """

        return JSONResponse(
            {"detail": "Authorization header missing or invalid"},
            status_code=status.HTTP_401_UNAUTHORIZED,
        )

    async def dispatch(self, request: Request, call_next: Callable):
        start_time = time.time()
        path = request.url.path

        # NOTE: CORS 사전 요청(Preflight Request)을 처리하기 위해 OPTIONS 메서드는 인증을 우회합니다.
        # 브라우저는 Authorization 헤더나 커스텀 헤더가 포함된 요청을 보내기 전에
        # 먼저 OPTIONS 메서드로 사전 요청을 보냅니다.
        # 이때 서버가 401/403을 반환하면 CORS 오류로 간주되어 실제 요청까지 도달하지 못합니다.
        # 따라서 OPTIONS 요청은 인증 없이 통과시킵니다.
        if request.method == "OPTIONS":
            return await call_next(request)

        is_system_path = any(
            path.startswith(system_path) for system_path in SYSTEM_PATHS
        )
        is_protected_path = any(
            path.startswith(protected_path) for protected_path in PROTECTED_PATHS
        )

        # HACK: 현재는 PROTECTED_PATHS와 SYSTEM_PATHS가 동일한 경로를 포함하고 있습니다.
        if is_system_path:
            try:
                token = self._extract_token_from_header(request)
            except ValueError:
                return self._handle_auth_error()

            # NOTE: 시스템(ex. Lambda) 환경에서는 다른 jwt 인증을 사용합니다.
            payload = decode_jwt(token=token)

            request.state.user_id = payload.get("sub")

        elif is_protected_path:
            try:
                token = self._extract_token_from_header(request)
            except ValueError:
                return self._handle_auth_error()

            user = decode_supabase_token(token)
            request.state.user_id = user.id

        try:
            response = await call_next(request)
            processing_time = round((time.time() - start_time) * 1000, 2)

            logger.info(
                {
                    "method": request.method,
                    "path": path,
                    "status_code": response.status_code,
                    "latency_ms": processing_time,
                    "user_id": getattr(request.state, "user_id", "anonymous"),
                    "user_agent": request.headers.get("user-agent", "unknown"),
                    "client_ip": request.headers.get(
                        "x-forwarded-for", request.client.host
                    ),
                }
            )
            return response

        except Exception as e:
            logger.error(
                {
                    "method": request.method,
                    "path": path,
                    "error": str(e),
                    "client_ip": request.client.host,
                    "user_id": getattr(request.state, "user_id", "anonymous"),
                }
            )
            return JSONResponse(
                {"detail": "Internal Server Error"},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
