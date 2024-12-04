from fastapi import APIRouter
from src.auth.schema import UpdatePassword
from src.auth.service import AuthService

auth_router = APIRouter()
auth_service = AuthService()


# 비밀번호 변경
@auth_router.put("/auth/password")
def update_user_password(item: UpdatePassword):

    return auth_service.update_user_password(item)


# """

# 사용자 회원가입 관련 논의
# 기획쪽은 어드민의 승인이 있는 후에 회원가입.
# 하지만 그럼 사용자 비밀번호를 암호화해서 저장하고 복호화해야하는 상황.
# 일단은 is_active로 진행중
# 대안방안
# 1. 암호화 복호화를 진행한다.(법적 문제도 확인해야함.)
# 2. 이미 등록한 계정이 승인 거부시 재승인을 할 수 있는 페이지 만들기
# 3. 어드민 이메일로 따로 요청 -> 이 경우 이를 위한 방안도 필요.


# """
