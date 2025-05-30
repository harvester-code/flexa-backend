from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.libs.dependencies import verify_token
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


@auth_router.get(
    "/me",
    summary="Get Current User Information",
    description="""
    Retrieve detailed information about the currently logged-in user.
    
    ### 사용 방법:
    1. 먼저 `/auth/login` 엔드포인트로 로그인하여 토큰을 발급받으세요
    2. 발급받은 토큰으로 Authorize 해주세요
    3. 이 엔드포인트를 호출하면 현재 로그인한 사용자의 정보를 볼 수 있습니다
    
    ### 반환 정보:
    - 기본 정보 (ID, 이메일, 전화번호)
    - 계정 생성/수정 시간
    - 이메일 확인 상태
    - 마지막 로그인 시간
    - 사용자 역할 및 메타데이터
    - 인증 관련 정보
    """,
    response_description="Returns detailed user information",
)
async def read_users_me(user=Depends(verify_token)):
    try:
        return {
            "id": user.id,
            "email": user.email,
            "phone": user.phone,
            "created_at": user.created_at,
            "updated_at": user.updated_at,
            "email_confirmed_at": user.email_confirmed_at,
            "last_sign_in_at": user.last_sign_in_at,
            "role": user.role,
            "aud": user.aud,
            "user_metadata": user.user_metadata,
            "app_metadata": user.app_metadata,
            "identities": user.identities,
            "factors": user.factors,
        }

    except Exception as _:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        )
