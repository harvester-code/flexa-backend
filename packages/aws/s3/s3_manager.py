import io
import json
from typing import Optional

import pandas as pd

from packages.doppler.client import get_secret
from .storage import get_s3_client, boto3_session


class S3Manager:
    """통합 S3 파일 관리자 - 모든 S3 작업의 단일 진입점"""

    def __init__(self):
        self.bucket_name = get_secret("AWS_S3_BUCKET_NAME")

    # ===============================
    # 비동기 메소드 (FastAPI용)
    # ===============================

    async def get_parquet_async(self, scenario_id: str, filename: str) -> Optional[pd.DataFrame]:
        """S3에서 parquet 파일 다운로드 (비동기)"""
        try:
            async with await get_s3_client() as s3_client:
                response = await s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=f"{scenario_id}/{filename}",
                )
                async with response["Body"] as stream:
                    data = await stream.read()
                    return pd.read_parquet(io.BytesIO(data))
        except Exception as e:
            # 상세한 로깅으로 디버깅 지원
            print(f"Error downloading parquet {filename} for {scenario_id}: {e}")
            return None

    async def get_json_async(self, scenario_id: str, filename: str) -> Optional[dict]:
        """S3에서 JSON 파일 다운로드 (비동기)"""
        try:
            async with await get_s3_client() as s3_client:
                response = await s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=f"{scenario_id}/{filename}",
                )
                async with response["Body"] as stream:
                    data = await stream.read()
                    return json.loads(data.decode("utf-8"))
        except Exception as e:
            print(f"Error downloading json {filename} for {scenario_id}: {e}")
            return None

    async def save_json_async(self, scenario_id: str, filename: str, data: dict):
        """S3에 JSON 파일 업로드 (비동기)"""
        try:
            json_content = json.dumps(data, ensure_ascii=False, indent=2)
            
            async with await get_s3_client() as s3_client:
                await s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=f"{scenario_id}/{filename}",
                    Body=json_content,
                    ContentType="application/json",
                    ContentEncoding="utf-8",
                )
                return True
        except Exception as e:
            print(f"Error uploading json {filename} for {scenario_id}: {e}")
            return False

    async def delete_json_async(self, scenario_id: str, filename: str) -> bool:
        """S3에서 JSON 파일 삭제 (비동기)"""
        try:
            async with await get_s3_client() as s3_client:
                await s3_client.delete_object(
                    Bucket=self.bucket_name,
                    Key=f"{scenario_id}/{filename}",
                )
                return True
        except Exception as e:
            print(f"Error deleting json {filename} for {scenario_id}: {e}")
            return False

    # ===============================
    # 동기 메소드 (Lambda, 배치용)
    # ===============================

    def get_parquet_sync(self, scenario_id: str, filename: str) -> Optional[pd.DataFrame]:
        """S3에서 parquet 파일 다운로드 (동기)"""
        try:
            s3_client = boto3_session.client("s3")
            response = s3_client.get_object(
                Bucket=self.bucket_name,
                Key=f"{scenario_id}/{filename}",
            )
            data = response["Body"].read()
            return pd.read_parquet(io.BytesIO(data))
        except Exception as e:
            print(f"Error downloading parquet {filename} for {scenario_id}: {e}")
            return None

    def save_json_sync(self, scenario_id: str, filename: str, data: dict):
        """S3에 JSON 파일 업로드 (동기)"""
        try:
            json_content = json.dumps(data, ensure_ascii=False, indent=2)
            s3_client = boto3_session.client("s3")
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=f"{scenario_id}/{filename}",
                Body=json_content,
                ContentType="application/json",
                ContentEncoding="utf-8",
            )
            return True
        except Exception as e:
            print(f"Error uploading json {filename} for {scenario_id}: {e}")
            return False

    def get_json_sync(self, scenario_id: str, filename: str) -> Optional[dict]:
        """S3에서 JSON 파일 다운로드 (동기)"""
        try:
            s3_client = boto3_session.client("s3")
            response = s3_client.get_object(
                Bucket=self.bucket_name,
                Key=f"{scenario_id}/{filename}",
            )
            data = response["Body"].read()
            return json.loads(data.decode("utf-8"))
        except Exception as e:
            print(f"Error downloading json {filename} for {scenario_id}: {e}")
            return None

    def delete_json_sync(self, scenario_id: str, filename: str) -> bool:
        """S3에서 JSON 파일 삭제 (동기)"""
        try:
            s3_client = boto3_session.client("s3")
            s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=f"{scenario_id}/{filename}",
            )
            return True
        except Exception as e:
            print(f"Error deleting json {filename} for {scenario_id}: {e}")
            return False
