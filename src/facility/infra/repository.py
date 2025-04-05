from src.facility.domain.repository import IFacilityRepository
from src.database import S3_SAVE_PATH
import awswrangler as wr
import boto3
import pandas as pd
import os


class FacilityRepository(IFacilityRepository):

    async def download_from_s3(
        self, session: boto3.Session, scenario_id: str
    ) -> pd.DataFrame:
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
