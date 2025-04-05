import random

from dependency_injector.wiring import inject
from sqlalchemy.ext.asyncio import AsyncSession
from supabase._async.client import AsyncClient as Client
from ulid import ULID

from src.auth.domain.auth import Certification, UserAccessRequest
from src.auth.domain.repository import IAuthRepository
from src.common import TimeStamp


class AuthService:
    @inject
    def __init__(
        self,
        auth_repo: IAuthRepository,
    ):
        self.auth_repo = auth_repo
        self.timestamp = TimeStamp()

    async def create_certification(self, db: AsyncSession, email: str):
        id = str(ULID())

        certification: Certification = Certification(
            id=id,
            email=email,
            cert_number=int(random.randint(1234, 9999)),
            expired_at=self.timestamp.time_now().add(minutes=5),
            created_at=self.timestamp.time_now(),
        )

        await self.auth_repo.create_certification(db, certification)

        return id

    async def fetch_certification(self, db: AsyncSession, id: str):
        now = self.timestamp.time_now()

        cert = await self.auth_repo.fetch_certification(db, id, now)

        return cert

    async def create_user(
        self,
        sb: Client,
        email: str,
        first_name: str,
        last_name: str,
        password: str,
    ):

        sign_up = {
            "email": email,
            "password": password,
            "options": {
                "data": {
                    "first_name": first_name,
                    "last_name": last_name,
                }
            },
        }
        user = await self.auth_repo.create_user(sb, sign_up)

        return {"message": {"status": 201, "result": user}}

    async def create_user_access_request(
        self,
        db: AsyncSession,
        user_email: str,
        admin_email: str,
        request_mg: str,
    ):

        id = self.ulid.generate()

        access: UserAccessRequest = UserAccessRequest(
            id=id,
            user_email=user_email,
            admin_email=admin_email,
            request_mg=request_mg,
            is_checked=False,
            created_at=self.timestamp.time_now(),
        )

        await self.auth_repo.create_user_access_request(db, access)

        return {"message": {"status": 201, "result": "success"}}

    async def login_user(self, db: AsyncSession, sb: Client, email: str, password: str):

        login = await self.auth_repo.login_user(db, sb, email, password)

        return {"message": {"status": 201, "result": login}}

    async def logout_user(self, sb: Client):

        await self.auth_repo.logout_user(sb)

        return {"message": {"status": 201, "result": "logout"}}

    async def reset_password(self, sb: Client, user_id, password):

        password = {"password": password}

        await self.auth_repo.reset_password(sb, user_id, password)

        return {"message": {"status": 201, "result": "reset_password"}}
