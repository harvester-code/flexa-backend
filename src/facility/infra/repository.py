from src.facility.domain.repository import IFacilityRepository
from src.database import S3_SAVE_PATH
import awswrangler as wr
import boto3
import pandas as pd
import os


class FacilityRepository(IFacilityRepository):

    async def download_from_s3(
        self, session: boto3.Session, filename: str
    ) -> pd.DataFrame:

        # df = wr.s3.read_parquet(
        #     path=f"{S3_SAVE_PATH}/tommie/test.parquet", boto3_session=session
        # )
        # FIXME: 이후에 실제 시뮬레이션 데이터로 변환
        env = os.getenv("ENVIRONMENT")

        if env == "local":
            parquet_path = "samples/sim_pax.parquet"

        elif env == "dev":
            parquet_path = "/code/samples/sim_pax.parquet"

        sample_data = os.path.join(os.getcwd(), parquet_path)
        df = pd.read_parquet(sample_data)
        return df
