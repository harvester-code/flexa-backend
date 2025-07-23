import io
import json
from typing import Optional

import aioboto3
import pandas as pd

from packages.constants import S3_BUCKET_NAME


class S3Downloader:
    """S3 downloader for simulation data and facility information"""

    def __init__(self, bucket_name: str = S3_BUCKET_NAME):
        self.bucket_name = bucket_name
        self.region_name = "ap-northeast-2"

    async def download_simulation_parquet_from_s3(
        self, scenario_id: str
    ) -> Optional[pd.DataFrame]:
        """Download simulation parquet file from S3"""

        session = aioboto3.Session()

        async with session.client("s3", region_name=self.region_name) as s3_client:
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

        async with session.client("s3", region_name=self.region_name) as s3_client:
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
