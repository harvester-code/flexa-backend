import awswrangler as wr
from typing import Optional
import json

from src.boto3_session import boto3_session
from src.home.domain.repository import IHomeRepository
from src.storages import check_s3_object_exists, get_s3_client


class HomeRepository(IHomeRepository):
    async def download_simulation_parquet_from_s3(self, scenario_id: str):
        return wr.s3.read_parquet(
            path=f"s3://flexa-dev-ap-northeast-2-data-storage/simulations/simulation-results-raw-data/{scenario_id}.parquet",
            boto3_session=boto3_session,
        )

    async def download_json_from_s3(self, scenario_id: str) -> Optional[dict]:
        try:
            bucket = "flexa-dev-ap-northeast-2-data-storage"
            key = f"simulations/facility-information-data/{scenario_id}.json"
            
            if not check_s3_object_exists(bucket, key):
                return None
                
            s3_client = get_s3_client()
            response = s3_client.get_object(Bucket=bucket, Key=key)
            return json.loads(response['Body'].read().decode('utf-8'))
            
        except Exception:
            return None


# 01JT0N8NYHG5SSEX8PM6RZWR26