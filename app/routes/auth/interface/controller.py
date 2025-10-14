from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from packages.supabase.auth import sign_in_with_password

auth_router = APIRouter(
    prefix="/auth",
    responses={
        401: {"description": "Unauthorized - Invalid credentials"},
        404: {"description": "Not Found - User not found"},
        500: {"description": "Internal Server Error"},
    },
)


class LoginRequest(BaseModel):
    email: str
    password: str


@auth_router.post(
    "/login",
    summary="Login with Supabase Account",
    description="""
    Login with your Supabase account to get an access token.
    
    ### 사용 방법:
    1. 이메일과 비밀번호를 입력하여 로그인
    2. 응답으로 받은 `access_token`을 복사
    3. FastAPI docs 에서 라우터별로 상단의 'Authorize' 버튼 클릭
    4. 발급받은 access_token 입력 및 Authorize 버튼 클릭
    
    ### 주의사항:
    - 토큰은 보안을 위해 안전하게 보관해야 합니다
    - 토큰이 노출되면 즉시 재발급 받으세요
    """,
    response_description="Returns access token on successful login",
)
async def login(login_data: LoginRequest):
    try:
        access_token = sign_in_with_password(login_data.email, login_data.password)

        return {"access_token": access_token, "token_type": "bearer"}

    except Exception as _:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
