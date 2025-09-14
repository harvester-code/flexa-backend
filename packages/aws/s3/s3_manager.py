import io
import json
from typing import Optional, List, Union
from botocore.exceptions import ClientError

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

    async def get_parquet_async(self, scenario_id: str, filename: str, as_dict: bool = False) -> Optional[Union[pd.DataFrame, List[dict]]]:
        """S3에서 parquet 파일 다운로드 (비동기)

        Args:
            scenario_id: 시나리오 ID
            filename: 파일명
            as_dict: True면 DataFrame을 dict 리스트로 변환하여 반환

        Returns:
            DataFrame 또는 dict 리스트 (as_dict=True인 경우)
        """
        try:
            async with await get_s3_client() as s3_client:
                response = await s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=f"{scenario_id}/{filename}",
                )
                async with response["Body"] as stream:
                    data = await stream.read()
                    df = pd.read_parquet(io.BytesIO(data))
                    return df.to_dict('records') if as_dict else df
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

    async def save_parquet_async(self, scenario_id: str, filename: str, df: pd.DataFrame) -> bool:
        """S3에 parquet 파일 업로드 (비동기)

        Args:
            scenario_id: 시나리오 ID
            filename: 파일명
            df: 저장할 DataFrame

        Returns:
            성공 여부
        """
        try:
            # DataFrame을 parquet 바이트로 변환
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, engine='pyarrow')
            buffer.seek(0)

            async with await get_s3_client() as s3_client:
                await s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=f"{scenario_id}/{filename}",
                    Body=buffer.getvalue(),
                    ContentType="application/octet-stream",
                )
                return True
        except Exception as e:
            print(f"Error uploading parquet {filename} for {scenario_id}: {e}")
            return False

    async def check_exists_async(self, scenario_id: str, filename: str) -> bool:
        """S3 객체 존재 여부 확인 (비동기)

        Args:
            scenario_id: 시나리오 ID
            filename: 파일명

        Returns:
            존재 여부
        """
        try:
            async with await get_s3_client() as s3_client:
                await s3_client.head_object(
                    Bucket=self.bucket_name,
                    Key=f"{scenario_id}/{filename}",
                )
                return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            print(f"Error checking existence of {filename} for {scenario_id}: {e}")
            return False
        except Exception as e:
            print(f"Error checking existence of {filename} for {scenario_id}: {e}")
            return False

    async def get_metadata_async(self, scenario_id: str, filename: str) -> Optional[dict]:
        """S3 객체 메타데이터 조회 (비동기)

        Args:
            scenario_id: 시나리오 ID
            filename: 파일명

        Returns:
            메타데이터 딕셔너리 (크기, 수정일 등)
        """
        try:
            async with await get_s3_client() as s3_client:
                response = await s3_client.head_object(
                    Bucket=self.bucket_name,
                    Key=f"{scenario_id}/{filename}",
                )
                return {
                    'size': response.get('ContentLength'),
                    'last_modified': response.get('LastModified'),
                    'content_type': response.get('ContentType'),
                    'etag': response.get('ETag'),
                }
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return None
            print(f"Error getting metadata for {filename} in {scenario_id}: {e}")
            return None
        except Exception as e:
            print(f"Error getting metadata for {filename} in {scenario_id}: {e}")
            return None

    async def list_files_async(self, scenario_id: str, prefix: str = "") -> List[str]:
        """S3 디렉토리 내 파일 목록 조회 (비동기)

        Args:
            scenario_id: 시나리오 ID
            prefix: 추가 prefix (선택)

        Returns:
            파일명 리스트
        """
        try:
            async with await get_s3_client() as s3_client:
                full_prefix = f"{scenario_id}/{prefix}" if prefix else f"{scenario_id}/"

                paginator = s3_client.get_paginator('list_objects_v2')
                page_iterator = paginator.paginate(
                    Bucket=self.bucket_name,
                    Prefix=full_prefix
                )

                files = []
                async for page in page_iterator:
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            # full path에서 scenario_id/ 부분 제거
                            file_path = obj['Key'].replace(f"{scenario_id}/", "", 1)
                            files.append(file_path)

                return files
        except Exception as e:
            print(f"Error listing files for {scenario_id}: {e}")
            return []

    # ===============================
    # 동기 메소드 (Lambda, 배치용)
    # ===============================

    def get_parquet_sync(self, scenario_id: str, filename: str, as_dict: bool = False) -> Optional[Union[pd.DataFrame, List[dict]]]:
        """S3에서 parquet 파일 다운로드 (동기)

        Args:
            scenario_id: 시나리오 ID
            filename: 파일명
            as_dict: True면 DataFrame을 dict 리스트로 변환하여 반환

        Returns:
            DataFrame 또는 dict 리스트 (as_dict=True인 경우)
        """
        try:
            s3_client = boto3_session.client("s3")
            response = s3_client.get_object(
                Bucket=self.bucket_name,
                Key=f"{scenario_id}/{filename}",
            )
            data = response["Body"].read()
            df = pd.read_parquet(io.BytesIO(data))
            return df.to_dict('records') if as_dict else df
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

    def save_parquet_sync(self, scenario_id: str, filename: str, df: pd.DataFrame) -> bool:
        """S3에 parquet 파일 업로드 (동기)

        Args:
            scenario_id: 시나리오 ID
            filename: 파일명
            df: 저장할 DataFrame

        Returns:
            성공 여부
        """
        try:
            # DataFrame을 parquet 바이트로 변환
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, engine='pyarrow')
            buffer.seek(0)

            s3_client = boto3_session.client("s3")
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=f"{scenario_id}/{filename}",
                Body=buffer.getvalue(),
                ContentType="application/octet-stream",
            )
            return True
        except Exception as e:
            print(f"Error uploading parquet {filename} for {scenario_id}: {e}")
            return False

    def check_exists_sync(self, scenario_id: str, filename: str) -> bool:
        """S3 객체 존재 여부 확인 (동기)

        Args:
            scenario_id: 시나리오 ID
            filename: 파일명

        Returns:
            존재 여부
        """
        try:
            s3_client = boto3_session.client("s3")
            s3_client.head_object(
                Bucket=self.bucket_name,
                Key=f"{scenario_id}/{filename}",
            )
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            print(f"Error checking existence of {filename} for {scenario_id}: {e}")
            return False
        except Exception as e:
            print(f"Error checking existence of {filename} for {scenario_id}: {e}")
            return False

    def get_metadata_sync(self, scenario_id: str, filename: str) -> Optional[dict]:
        """S3 객체 메타데이터 조회 (동기)

        Args:
            scenario_id: 시나리오 ID
            filename: 파일명

        Returns:
            메타데이터 딕셔너리 (크기, 수정일 등)
        """
        try:
            s3_client = boto3_session.client("s3")
            response = s3_client.head_object(
                Bucket=self.bucket_name,
                Key=f"{scenario_id}/{filename}",
            )
            return {
                'size': response.get('ContentLength'),
                'last_modified': response.get('LastModified'),
                'content_type': response.get('ContentType'),
                'etag': response.get('ETag'),
            }
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return None
            print(f"Error getting metadata for {filename} in {scenario_id}: {e}")
            return None
        except Exception as e:
            print(f"Error getting metadata for {filename} in {scenario_id}: {e}")
            return None

    def list_files_sync(self, scenario_id: str, prefix: str = "") -> List[str]:
        """S3 디렉토리 내 파일 목록 조회 (동기)

        Args:
            scenario_id: 시나리오 ID
            prefix: 추가 prefix (선택)

        Returns:
            파일명 리스트
        """
        try:
            s3_client = boto3_session.client("s3")
            full_prefix = f"{scenario_id}/{prefix}" if prefix else f"{scenario_id}/"

            paginator = s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=full_prefix
            )

            files = []
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # full path에서 scenario_id/ 부분 제거
                        file_path = obj['Key'].replace(f"{scenario_id}/", "", 1)
                        files.append(file_path)

            return files
        except Exception as e:
            print(f"Error listing files for {scenario_id}: {e}")
            return []
