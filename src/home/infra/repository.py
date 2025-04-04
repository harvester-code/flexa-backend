from src.home.domain.repository import IHomeRepository
from src.database import get_boto3_session, aget_supabase_client
import pandas as pd
import awswrangler as wr
import os
import numpy as np
from src.database import S3_SAVE_PATH
import boto3


class HomeRepository(IHomeRepository):
    def __init__(self):
        self.session = get_boto3_session()

    async def download_from_s3(self, session: boto3.Session, scenario_id: str):
        env = os.getenv("ENVIRONMENT")

        if env == "local":
            parquet_path = "samples/sim_pax.parquet"
            sample_data = os.path.join(os.getcwd(), parquet_path)
            df = pd.read_parquet(sample_data)

        elif env == "dev":
            df = wr.s3.read_parquet(
                path=f"{S3_SAVE_PATH}/dev/{scenario_id}.parquet", boto3_session=session
            )

        return df
