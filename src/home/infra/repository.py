from src.home.domain.repository import IHomeRepository
from src.database import get_boto3_session, aget_supabase_client
import pandas as pd
import awswrangler as wr
import os
from fastapi import Request


class HomeRepository(IHomeRepository):
    def __init__(self):
        self.boto3_session = get_boto3_session()

    async def login_supabase(self, email: str, password: str):
        supabase = await aget_supabase_client()
        try:
            response = await supabase.auth.sign_in_with_password(
                {"email": email, "password": password}
            )
            return {
                "message": "로그인 성공",
                "access_token": response.session.access_token,
                "refresh_token": response.session.refresh_token,
                "user": response.user,
            }
        except Exception as e:
            return {"message": "로그인 실패", "error": str(e)}

    async def fetch_supabase_data(self, user_id: str):
        supabase = await aget_supabase_client()
        try:
            data = (
                await supabase.table("user_info")
                .select("*")
                .eq("id", user_id)
                .execute()
            )
            return {"message": "성공", "data": data}
        except Exception as e:
            return {"message": "Supabase 데이터 조회 실패", "error": str(e)}

    async def fetch_simulation_files(self, request: Request):
        try:
            s3_client = self.boto3_session.client("s3")
            bucket_name = "flexa-prod-ap-northeast-2-data-storage"
            prefix = "simulations/"  # 원하는 폴더 경로

            # S3 버킷의 객체 목록 조회
            response = s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix=prefix, MaxKeys=10  # 최대 10개의 객체만 조회
            )

            # 파일 목록 추출
            files = []
            if "Contents" in response:
                for obj in response["Contents"]:
                    files.append(
                        {
                            "key": obj["Key"],
                            "size": obj["Size"],
                            "last_modified": obj["LastModified"].isoformat(),
                        }
                    )

            return {
                "message": "S3 데이터 조회 성공",
                "bucket": bucket_name,
                "prefix": prefix,
                "files": files,
            }
        except Exception as e:
            return {"message": "S3 데이터 조회 실패", "error": str(e)}

    async def fetch_simulation_summary(self, request: Request, file_id: str):
        # 방법1. S3에서 직접 읽기
        bucket_name = os.getenv("AWS_S3_BUCKET_NAME")
        file_key = f"simulations/tommie/test.parquet"
        file_path = f"s3://{bucket_name}/{file_key}"
        df = wr.s3.read_parquet(path=file_path, boto3_session=self.boto3_session)

        # # 방법2. 로컬 파일에서 직접 parquet 읽기
        # file_path = "samples/test.parquet"
        # df = pd.read_parquet(file_path)

        return_dict = {
            "terminal_overview": {
                "start_time": pd.to_datetime(df["show_up_time"].min()).strftime(
                    "%Y-%m-%d %H:00:00"
                ),
                "end_time": pd.to_datetime(df["show_up_time"].max()).strftime(
                    "%Y-%m-%d %H:00:00"
                ),
            },
            "summary": {
                "departure_flights": df["flight_number"].nunique(),
                "arrival_flights": 0,
                "delay_flights": df[df["gate_departure_delay"] > 15][
                    "flight_number"
                ].nunique(),
                "return_flights": int(df["is_cancelled"].sum()),
                "departure_pax": len(df),
                "arrival_pax": 0,
                "transfer_pax": 0,
            },
        }
        return return_dict
