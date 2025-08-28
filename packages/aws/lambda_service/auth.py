"""
AWS Lambda 인증 모듈

이 모듈은 Lambda ↔ API 간 시스템 인증을 담당합니다.
Lambda가 시뮬레이션 완료/오류 결과를 API로 전송할 때 사용되는 JWT 검증 기능을 제공합니다.

시스템 인증 플로우:
1. Lambda가 SYSTEM_JWT_SECRET_KEY로 JWT 생성
2. Lambda가 API의 시스템 전용 엔드포인트 호출
3. API가 decode_jwt()로 Lambda 신원 확인
"""

import jwt
from fastapi import HTTPException, status
from jwt.exceptions import ExpiredSignatureError, InvalidTokenError

from packages.doppler.client import get_secret

SECRET = get_secret("SYSTEM_JWT_SECRET_KEY")
ALGORITHM = "HS256"


def decode_jwt(token: str) -> dict:
    """
    Lambda 시스템 JWT 토큰 검증

    Lambda에서 전송된 JWT 토큰을 검증하고 페이로드를 반환합니다.
    일반 사용자 인증과는 별도의 시스템 간 통신용 인증입니다.

    Args:
        token: JWT 토큰 문자열

    Returns:
        dict: 검증된 JWT 페이로드 (user_id 등 포함)

    Raises:
        HTTPException: 토큰이 만료되었거나 유효하지 않은 경우
        ValueError: SYSTEM_JWT_SECRET_KEY가 설정되지 않은 경우
    """

    if not SECRET:
        raise ValueError("SYSTEM_JWT_SECRET_KEY environment variable is not set")

    try:
        payload = jwt.decode(token, SECRET, algorithms=[ALGORITHM])
        return payload
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="System token has expired"
        )
    except InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid system token"
        )
