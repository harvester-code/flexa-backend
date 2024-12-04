from supabase import Client, AuthApiError
from fastapi import HTTPException
import random

from src.database import supabase_public_clinet
from src.users.schema import (
    UserInfo,
    RequestAccess,
)
from src.common import TimeStamp

timestamp = TimeStamp()


supabase: Client = supabase_public_clinet()


class UserService:

    def create_certification(self, item: UserInfo):

        response = (
            supabase.schema("users")
            .table("certification")
            .insert(
                {
                    "email": item.email.strip(),
                    "certification_number": int(random.randint(1234, 9999)),
                    "expired_at": timestamp.time_now()
                    .add(minutes=5)
                    .to_iso8601_string(),
                }
            )
            .execute()
        )

        return {"message": {"status": 201, "result": response.data}}

    def fetch_certification(self, id):

        response = (
            supabase.schema("users")
            .table("certification")
            .select("certification_number")
            .eq("id", id)
            .gt("expired_at", timestamp.time_now().to_iso8601_string())
            .execute()
        )

        result = {
            "message": {
                "status": 204,
                # TODO: 외주사랑 기획이랑 얘기해서 어떻게 모달을 제공할건지.
                "result": None,  # 화면에 반환할 값을 보낸다.
            }
        }

        if len(response.data) > 0:
            result["message"]["status"] = 200
            result["message"]["result"] = response.data[0]["certification_number"]

        return result

    def create_user(self, item: UserInfo):

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

    def requset_access(self, item: RequestAccess):

        # TODO: request_page를 json형태로해서 true false 형태로
        # {"security": {"label": "Security", "value": True}...}

        response = (
            supabase.schema("users")
            .table("request_access")
            .insert(
                {
                    "user_id": item.user_id,
                    "admin_email": item.admin_email,
                    "request_mg": item.request_mg,
                    "request_page": item.request_page,
                }
            )
            .execute()
        )

        return {"message": {"status": 201, "result": response.data}}

    def login_user(self, item: UserInfo):

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

    def redirect_reset_password(self, item: UserInfo):

        response = supabase.auth.reset_password_for_email(
            item.email,
            {
                "redirect_to": "https://example.com/update-password",
            },
        )

        return {"message": {"status": 200, "result": response}}
