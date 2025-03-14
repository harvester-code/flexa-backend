from src.admin.domain.repository import IAdminRepository
from src.database import S3_SAVE_PATH
import awswrangler as wr
import boto3
import pandas as pd


class AdminRepository(IAdminRepository):

    async def download_from_s3(
        self, session: boto3.Session, filename: str
    ) -> pd.DataFrame:

        sim_df = wr.s3.read_parquet(
            path=f"{S3_SAVE_PATH}/{filename}", boto3_session=session
        )

        return sim_df
