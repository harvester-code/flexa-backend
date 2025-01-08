from datetime import datetime
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from supabase._async.client import AsyncClient as Client

from src.auth.domain.auth import (
    Certification as CertificationVO,
)
from src.auth.domain.auth import (
    UserAccessRequest as UserAccessRequestVO,
)
from src.auth.domain.repository import IAuthRepository
from src.auth.infra.models import Certification, UserAccessRequest


class AuthRepository(IAuthRepository):

    async def create_certification(
        self, db: AsyncSession, certification: CertificationVO
    ):

        new_cert = Certification(
            id=certification.id,
            email=certification.email,
            cert_number=certification.cert_number,
            expired_at=certification.expired_at,
            created_at=certification.created_at,
        )

        db.add(new_cert)
        await db.commit()

    async def fetch_certification(
        self, db: AsyncSession, id: str, now: datetime
    ) -> list[CertificationVO]:

        result = await db.execute(
            select(Certification)
            .where(Certification.id == id)
            .where(Certification.expired_at >= now)
        )

        cert = result.scalar_one_or_none()

        return cert

    async def create_user(self, sb: Client, sign_up: dict):

        user = await sb.auth.sign_up(sign_up)
        return user

    async def create_user_access_request(
        self, db: AsyncSession, access: UserAccessRequestVO
    ):

        new_access = UserAccessRequest(
            id=access.id,
            user_email=access.user_email,
            admin_email=access.admin_email,
            request_mg=access.request_mg,
            is_checked=access.is_checked,
            created_at=access.created_at,
        )

        db.add(new_access)
        await db.commit()

    # TODO: supabase로 되어있는 쿼리들을 sqlalchemy로 고치기
    async def login_user(self, db: AsyncSession, sb: Client, email: str, password: str):

        sign = await sb.table("user_info").select("email").eq("email", email).execute()

        if len(sign.data) == 0:
            raise HTTPException(status_code=400, detail="Check your email or sign up")

        result = await db.execute(
            select(UserAccessRequest.is_checked).where(
                UserAccessRequest.user_email == email
            )
        )

        check = result.scalar_one_or_none()

        if check is False:
            raise HTTPException(
                status_code=400, detail="Wait your admin access request"
            )

        check = (
            await sb.table("user_info").select("is_active").eq("email", email).execute()
        )

        if check.data[0]["is_active"] is False:
            raise HTTPException(status_code=400, detail="Your email is not accessible")

        login = await sb.auth.sign_in_with_password(
            {"email": email, "password": password}
        )

        return login

    async def logout_user(self, sb: Client):

        await sb.auth.sign_out()

    async def reset_password(self, sb: Client, user_id: str, password: dict):

        await sb.auth.admin.update_user_by_id(
            user_id,
            password,
        )
