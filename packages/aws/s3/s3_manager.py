import io
import json
from typing import Optional, List, Union
from botocore.exceptions import ClientError
from loguru import logger

import pandas as pd

from packages.doppler.client import get_secret
from .storage import get_s3_client


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
            logger.error(f"[S3] Error downloading parquet {filename} for {scenario_id}: {e}")
            return None

    async def get_json_async(self, scenario_id: str, filename: str) -> Optional[dict]:
        """S3에서 JSON 파일 다운로드 (비동기)"""
        try:
            key = f"{scenario_id}/{filename}"
            logger.debug(f"[S3] GET s3://{self.bucket_name}/{key}")
            async with await get_s3_client() as s3_client:
                response = await s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=key,
                )
                async with response["Body"] as stream:
                    data = await stream.read()
                    result = json.loads(data.decode("utf-8"))
                    logger.debug(f"[S3] Successfully downloaded JSON ({len(data)} bytes)")
                    return result
        except Exception as e:
            logger.error(f"[S3] Error downloading json {filename} for {scenario_id}: {e}")
            return None

    async def save_json_async(self, scenario_id: str, filename: str, data: dict):
        """S3에 JSON 파일 업로드 (비동기)"""
        try:
            json_content = json.dumps(data, ensure_ascii=False, indent=2)
            key = f"{scenario_id}/{filename}"
            logger.debug(f"[S3] PUT s3://{self.bucket_name}/{key} ({len(json_content)} bytes)")
            
            async with await get_s3_client() as s3_client:
                await s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=key,
                    Body=json_content,
                    ContentType="application/json",
                    ContentEncoding="utf-8",
                )
                logger.debug(f"[S3] Successfully uploaded JSON to S3")
                return True
        except Exception as e:
            logger.error(f"[S3] Error uploading json {filename} for {scenario_id}: {e}")
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
            logger.error(f"[S3] Error deleting json {filename} for {scenario_id}: {e}")
            return False

    async def delete_scenario_data(self, scenario_id: str) -> bool:
        """시나리오의 모든 S3 데이터 삭제 (비동기)

        Args:
            scenario_id: 삭제할 시나리오 ID

        Returns:
            성공 여부
        """
        try:
            async with await get_s3_client() as s3_client:
                # 시나리오 폴더 내 모든 객체 나열
                paginator = s3_client.get_paginator('list_objects_v2')
                prefix = f"{scenario_id}/"

                delete_keys = []
                async for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                    if 'Contents' in page:
                        delete_keys.extend([{'Key': obj['Key']} for obj in page['Contents']])

                # 객체가 있으면 삭제
                if delete_keys:
                    await s3_client.delete_objects(
                        Bucket=self.bucket_name,
                        Delete={'Objects': delete_keys}
                    )
                    logger.info(f"[S3] Deleted {len(delete_keys)} objects for scenario {scenario_id}")

                return True
        except Exception as e:
            logger.error(f"[S3] Error deleting scenario data for {scenario_id}: {e}")
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
            logger.error(f"[S3] Error uploading parquet {filename} for {scenario_id}: {e}")
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
            logger.error(f"[S3] Error checking existence of {filename} for {scenario_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"[S3] Error checking existence of {filename} for {scenario_id}: {e}")
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
            key = f"{scenario_id}/{filename}"
            logger.debug(f"[S3] HEAD s3://{self.bucket_name}/{key}")
            async with await get_s3_client() as s3_client:
                response = await s3_client.head_object(
                    Bucket=self.bucket_name,
                    Key=key,
                )
                metadata = {
                    'size': response.get('ContentLength'),
                    'last_modified': response.get('LastModified'),
                    'content_type': response.get('ContentType'),
                    'etag': response.get('ETag'),
                }
                logger.debug(f"[S3] Metadata retrieved: last_modified={metadata['last_modified']}")
                return metadata
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.debug(f"[S3] File not found (404): {scenario_id}/{filename}")
                return None
            logger.error(f"[S3] Error getting metadata for {filename} in {scenario_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"[S3] Error getting metadata for {filename} in {scenario_id}: {e}")
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
            logger.error(f"[S3] Error listing files for {scenario_id}: {e}")
            return []

    async def copy_scenario_data(self, source_scenario_id: str, target_scenario_id: str) -> bool:
        """S3 시나리오 데이터 복사 (비동기)

        원본 시나리오의 모든 파일을 새 시나리오로 복사합니다.

        Args:
            source_scenario_id: 원본 시나리오 ID
            target_scenario_id: 대상 시나리오 ID

        Returns:
            성공 여부
        """
        try:
            # 1. 원본 시나리오의 모든 파일 목록 조회
            files = await self.list_files_async(source_scenario_id)

            if not files:
                logger.info(f"[S3] No files to copy for scenario {source_scenario_id}")
                return True  # 복사할 파일이 없어도 성공으로 처리

            # 2. 각 파일을 복사
            async with await get_s3_client() as s3_client:
                for file_path in files:
                    source_key = f"{source_scenario_id}/{file_path}"
                    target_key = f"{target_scenario_id}/{file_path}"

                    try:
                        # S3 복사 작업
                        copy_source = {
                            'Bucket': self.bucket_name,
                            'Key': source_key
                        }

                        await s3_client.copy_object(
                            CopySource=copy_source,
                            Bucket=self.bucket_name,
                            Key=target_key
                        )

                        logger.debug(f"[S3] Copied: {source_key} → {target_key}")

                    except Exception as file_error:
                        logger.warning(f"[S3] Failed to copy {file_path}: {file_error}")
                        # 개별 파일 복사 실패는 경고만 기록하고 계속 진행

            logger.info(f"[S3] Data copy completed: {source_scenario_id} → {target_scenario_id}")
            return True

        except Exception as e:
            logger.error(f"[S3] Error copying scenario data from {source_scenario_id} to {target_scenario_id}: {e}")
            return False
