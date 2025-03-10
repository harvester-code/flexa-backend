from supabase import Client

from src.database import supabase_public_clinet
from src.managements.schema import UpdateFileName

supabase: Client = supabase_public_clinet()


class ManagementService:

    def fetch_simulation_info(self, user_id, simulated_at=None):

        query = (
            supabase.schema("users")
            .table("simulation_management")
            .select("*")
            .eq("user_id", user_id)
            .eq(
                "simulation_hierarchy.parent_id",
                (
                    supabase.table("simulation_management")
                    .select("id")
                    .eq("user_id", user_id)
                    .eq("name", "root")
                    .execute()
                    .data[0]["id"]
                ),
            )
            .eq("simulation_hierarchy.depth", 1)
        )

        if simulated_at:
            query = query.filter("simulated_at", "gte", simulated_at[0]).filter(
                "simulated_at", "lte", simulated_at[1]
            )

        response = query.execute()

        return {"message": {"status": 200, "result": response.data}}

    def fetch_filter_info(self, user_id, folder_id, page, category, terminal):

        query = (
            supabase.schema("users")
            .table("filter_management")
            .select("*")
            .eq("user_id", user_id)
            .eq(
                "filter_hierarchy.parent_id",
                (
                    supabase.table("filter_management")
                    .select("id")
                    .eq("user_id", user_id)
                    .eq("name", "root")
                    .execute()
                    .data[0]["id"]
                ),
            )
            .eq("filter_hierarchy.depth", 1)
        )

        if page:
            query = query.eq("page", page)

        if category:
            query = query.eq("category", category)

        if terminal:
            query = query.eq("terminal", terminal)

        response = query.execute()

        return {"message": {"status": 200, "result": response.data}}

    def delete_file(self, category, name):

        query = (
            supabase.schema("users")
            .table(f"{category}_management")
            .delete()
            .eq(f"{category}_name", name)
        )

        response = query.execute()

        return {"message": {"status": 201, "result": response.data}}

    def update_file_name(self, item: UpdateFileName):

        query = (
            supabase.schema("users")
            .table(f"{item.category}_management")
            .update({f"{item.category}_name": item.new_name})
            .eq(f"{item.category}_name", item.old_name)
        )

        response = query.execute()

        return {"message": {"status": 201, "result": response.data}}

    def move_file(self, parent_id, child_id, category):

        response = (
            supabase.schema("users")
            .table(f"{category}_hierarchy")
            .select("*")
            .eq("child_id", child_id)
            .execute()
        )

        depth = len(response.data)

        response = (
            supabase.schema("users")
            .table(f"{category}_hierarchy")
            .insert({"parent_id": parent_id, "child_id": child_id, "depth": depth + 1})
            .execute()
        )
        return {"message": {"status": 201, "result": response.data}}

    def replicated_file():
        # 복제할 시 새로운 S3 파일을 만들어줘야함...
        ...

    def sharing_file():
        # 공유할 시 새로운 S3 파일을 만들어줘야함...
        ...

    # 분석페이지 이동하는 API 만들어야함.
