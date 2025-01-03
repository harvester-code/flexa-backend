import random
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from supabase import AuthApiError, Client

from src.common import TimeStamp
from src.database import supabase_public_clinet
from src.users.queries import (
    INSERT_CERTIFICATION,
    GET_CERTIFICATION,
    INSERT_REQUEST_ACCESS,
)
from src.users.schema import Certification, UserCreate, RequestAccess

timestamp = TimeStamp()
# TODO: 익셉션 함수 모아놓기

supabase: Client = supabase_public_clinet()


class UserService:

    def create_certification(self, item: Certification, db: Session):

        id = uuid4()
        params = {
            "id": id,
            "email": item.email,
            "certification_number": int(random.randint(1234, 9999)),
            "expired_at": timestamp.time_now().add(minutes=5).to_iso8601_string(),
        }

        try:
            db.execute(text(INSERT_CERTIFICATION), params)
            db.commit()

            return {"message": {"status": 201, "result": id}}

        except Exception as e:
            return f"fail {e}"

    def fetch_certification(self, id, db: Session):

        params = {"id": id, "now": timestamp.time_now().to_iso8601_string()}

        try:
            result = db.execute(text(GET_CERTIFICATION), params).fetchone()
            return {"message": {"status": 201, "result": result[0]}}

        except Exception as e:
            return e

    def create_user(self, item: UserCreate):

        response = supabase.auth.sign_up(
            {
                "email": item.email,
                "password": item.password,
                "options": {
                    "data": {
                        "first_name": item.first_name,
                        "last_name": item.last_name,
                    }
                },
            }
        )

        return {"message": {"status": 201, "result": response}}

    def requset_access(self, item: RequestAccess, db: Session):

        params = {
            "user_id": item.user_id,
            "admin_email": item.admin_email,
            "request_mg": item.request_mg,
        }

        try:
            db.execute(text(INSERT_REQUEST_ACCESS), params)
            db.commit()

            return {"message": {"status": 201, "result": "success"}}

        except Exception as e:
            return e

    def login_user(self, item):

        query = (
            supabase.schema("users")
            .table("user_info")
            .select("is_active")
            .eq("email", item.email)
        )

        response = query.execute()

        if len(response.data) == 0:
            raise HTTPException(status_code=400, detail="Check your email or sign up")

        response = query.eq("is_active", True).execute()

        if len(response.data) == 0:

            response = (
                supabase.schema("users")
                .table("request_access")
                .select("is_checked")
                .eq("is_checked", True)
                .execute()
            )

            if len(response.data) == 0:
                raise HTTPException(
                    status_code=400, detail="Wait your admin access request"
                )

            raise HTTPException(status_code=400, detail="Your email is not accessible")

        try:
            response = supabase.auth.sign_in_with_password(
                {"email": item.email, "password": item.password}
            )

            return {"message": {"status": 200, "result": response}}

        except AuthApiError as error:
            raise HTTPException(status_code=400, detail=f"{error}")

    def logout_user(self):

        response = supabase.auth.sign_out()

        return {"message": {"status": 200, "result": response}}

    def redirect_reset_password(self, item):

        response = supabase.auth.reset_password_for_email(
            item.email,
            {
                "redirect_to": "https://example.com/update-password",
            },
        )

        return {"message": {"status": 200, "result": response}}
