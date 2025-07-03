from fastapi import HTTPException, status

from packages.supabase.client import get_supabase_client


def decode_supabase_token(token: str):
    """
    Supabase 토큰을 디코딩하고 사용자 ID를 반환합니다.
    """

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid token",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        supabase = get_supabase_client()
        user = supabase.auth.get_user(token)

        if not user or not user.user:
            raise credentials_exception

        return user.user
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Decode supabase token error: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )


def sign_in_with_password(email: str, password: str):
    """
    Supabase에 이메일과 비밀번호로 로그인하고 토큰을 반환합니다.
    """

    supabase = get_supabase_client()
    response = supabase.auth.sign_in_with_password(
        {"email": email, "password": password}
    )

    if not response.session or not response.session.access_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return response.session.access_token
