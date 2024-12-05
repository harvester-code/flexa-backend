from supabase import Client

from src.auth.schema import UpdatePassword
from src.database import supabase_auth_client

supabase: Client = supabase_auth_client()


class AuthService:

    def update_user_password(self, item: UpdatePassword):

        supabase.auth.admin.update_user_by_id(
            item.user_id,
            {"password": item.password},
        )

        return {"message": {"status": 201, "result": "Change Password"}}
