from src.home.domain.repository import IHomeRepository
from src.database import get_boto3_session, aget_supabase_client
import pandas as pd
import awswrangler as wr
import os
import numpy as np


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

    async def fetch_supabase_data(self):
        supabase = await aget_supabase_client()
        try:
            data = await supabase.table("user_info").select("*").execute()
            return {"message": "성공", "data": data}
        except Exception as e:
            return {"message": "Supabase 데이터 조회 실패", "error": str(e)}

    async def fetch_simulation_files(self):
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
                    # 파일 키에서 ID 추출
                    key_parts = obj["Key"].split("/")
                    if len(key_parts) >= 3:
                        file_id = key_parts[1]  # simulations/<file_id>/filename.parquet
                    else:
                        file_id = "unknown"

                    files.append(
                        {
                            "file_id": file_id,
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

    async def fetch_simulation_summary(self, file_id: str):
        """시뮬레이션 요약 정보를 조회합니다."""

        # bucket_name = os.getenv("AWS_S3_BUCKET_NAME")
        # file_key = f"simulations/tommie/{file_id}.parquet"
        # file_path = f"s3://{bucket_name}/{file_key}"
        # df = wr.s3.read_parquet(path=file_path, boto3_session=self.boto3_session)

        df = pd.read_csv("samples/sim_pax.csv")
        return df
