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
        df = pd.read_parquet("samples/v1_sim_pax.parquet")
        # FIXME: 이후에 실제 시뮬레이션 데이터로 변환
        # df = wr.s3.read_parquet(
        #     path=f"{S3_SAVE_PATH}/tommie/test.parquet", boto3_session=session
        # )
        return df
