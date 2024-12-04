from supabase import Client
from src.database import supabase_public_clinet
from src.admins.schema import RequestUser
from typing import Union
from datetime import datetime

supabase: Client = supabase_public_clinet()


class AdminService:

    def fetch_request_user_info(self, amdin_id):

        response = (
            supabase.schema("users")
            .table("request_user_info")
            .select("*")
            .eq("admin_id", amdin_id)
            .execute()
        )

        return {"message": {"status": 200, "result": response.data}}

    def update_request_deactive(self, item: RequestUser):

        response = (
            supabase.schema("users")
            .table("user_info")
            .select("user_id")
            .eq("email", item.user_email)
            .execute()
        )

        user_id = response.data[0]["user_id"]

        (
            supabase.schema("users")
            .table("request_access")
            .update(
                {
                    "is_checked": True,
                }
            )
            .eq("user_id", user_id)
            .execute()
        )

        response = {"user_id": user_id, "is_checked": True}

        return {"message": {"status": 200, "result": response}}

    def approve_user_sign_up(self, item: RequestUser, permission_result: list):

        response = (
            supabase.schema("users")
            .table("user_info")
            .select("group_id")
            .eq("user_id", item.admin_id)
            .execute()
        )

        group_id = response.data[0]["group_id"]

        response = (
            supabase.schema("users")
            .table("user_info")
            .update(
                {
                    "group_id": group_id,
                    "role_id": 2,
                    "is_active": True,
                }
            )
            .eq("email", item.user_email)
            .execute()
        )

        result = {
            "message": {
                "status": 200,
                "result": {
                    "user_info": response.data,
                    "permission": permission_result,
                },
            }
        }

        return result

    def update_user_permission(self, item: RequestUser, user_id):

        # NEW에 대한 고민 필요..
        if user_id is None:
            response = (
                supabase.schema("users")
                .table("user_info")
                .select("user_id")
                .eq("email", item.user_email)
                .execute()
            )

            user_id = response.data[0]["user_id"]

        response = (
            supabase.schema("users")
            .table("user_permissions")
            .delete()
            .eq("user_id", user_id)
            .execute()
        )

        permission_list = item.user_permissions
        insert_list = []
        for permission in permission_list:

            response = (
                supabase.schema("users")
                .table("permissions")
                .select("id")
                .eq("permission_name", str(permission))
                .execute()
            )

            permission_id = response.data[0]["id"]
            insert_value = {"user_id": user_id, "permission_id": permission_id}
            insert_list.append(insert_value)

        response = (
            supabase.schema("users")
            .table("user_permissions")
            .insert(insert_list)
            .execute()
        )

        return {"message": {"status": 200, "result": response.data}}

    def fetch_user_management(self, admin_id):

        response = (
            supabase.schema("users")
            .table("user_info")
            .select("group_id")
            .eq("user_id", admin_id)
            .execute()
        )

        group_id = response.data[0]["group_id"]

        response = (
            supabase.schema("users")
            .table("user_management")
            .select("*")
            .eq("group_id", group_id)
            .execute()
        )

        return {"message": {"status": 200, "result": response.data}}

    def deactive_user(self, user_email):

        response = (
            supabase.schema("users")
            .table("user_info")
            .update(
                {
                    "is_active": False,
                }
            )
            .eq("email", user_email)
            .execute()
        )

        return {"message": {"status": 201, "result": response.data}}
