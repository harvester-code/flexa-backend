from typing import Callable

from fastapi import Request, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from packages.supabase.auth import decode_supabase_token
from packages.constants import API_PREFIX

PROTECTED_PATHS = [
    f"{API_PREFIX}/simulations",
    f"{API_PREFIX}/homes",
    f"{API_PREFIX}/facilities",
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

        if any(path.startswith(protected_path) for protected_path in PROTECTED_PATHS):
            auth_header = request.headers.get("Authorization")

            if not auth_header or not auth_header.startswith("Bearer "):
                return JSONResponse(
                    {"detail": "Authorization header missing or invalid"},
                    status_code=status.HTTP_401_UNAUTHORIZED,
                )

            token = auth_header.split(" ")[1]

            try:
                user = decode_supabase_token(token)
                request.state.user_id = user.id
            except Exception as e:
                return JSONResponse(
                    {"detail": str(e)},
                    status_code=status.HTTP_401_UNAUTHORIZED,
                )

        return await call_next(request)
