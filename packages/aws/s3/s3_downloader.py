import io
import json
from typing import Optional

import aioboto3
import pandas as pd

from packages.secrets import get_secret


class S3Downloader:
    """S3 downloader for simulation data and facility information"""

    def __init__(self, bucket_name: str = "flexa-dev-ap-northeast-2-data-storage"):
        self.bucket_name = bucket_name
        self.aws_access_key_id = get_secret("AWS_ACCESS_KEY")
        self.aws_secret_access_key = get_secret("AWS_SECRET_ACCESS_KEY")
        self.region_name = "ap-northeast-2"

    async def download_simulation_parquet_from_s3(
        self, scenario_id: str
    ) -> Optional[pd.DataFrame]:
        """Download simulation parquet file from S3"""

        session = aioboto3.Session()

        async with session.client(
            "s3",
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        ) as s3_client:
            try:
                response = await s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=f"simulations/simulation-results-raw-data/{scenario_id}.parquet",
                )
                async with response["Body"] as stream:
                    data = await stream.read()
                    return pd.read_parquet(io.BytesIO(data))
            except Exception:
                return None

    async def download_facility_json_from_s3(self, scenario_id: str) -> Optional[dict]:
        """Download facility information JSON file from S3"""

        session = aioboto3.Session()

        async with session.client(
            "s3",
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        ) as s3_client:
            try:
                response = await s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=f"simulations/facility-information-data/{scenario_id}.json",
                )
                async with response["Body"] as stream:
                    data = await stream.read()
                    return json.loads(data.decode("utf-8"))
            except Exception:
                return None
