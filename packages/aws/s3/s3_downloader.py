import io
import json
from typing import Optional

import boto3
import pandas as pd
import os


class S3Downloader:
    """S3 downloader for simulation data and facility information"""
    
    def __init__(self, bucket_name: str = "flexa-dev-ap-northeast-2-data-storage"):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            "s3",
            region_name="ap-northeast-2",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )

    def download_simulation_parquet_from_s3(self, scenario_id: str) -> Optional[pd.DataFrame]:
        """Download simulation parquet file from S3"""
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=f"simulations/simulation-results-raw-data/{scenario_id}.parquet"
            )
            return pd.read_parquet(io.BytesIO(response["Body"].read()))
        except Exception:
            return None

    def download_facility_json_from_s3(self, scenario_id: str) -> Optional[dict]:
        """Download facility information JSON file from S3"""
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=f"simulations/facility-information-data/{scenario_id}.json"
            )
            return json.loads(response["Body"].read().decode("utf-8"))
        except Exception:
            return None 