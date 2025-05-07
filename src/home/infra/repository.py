import awswrangler as wr
import boto3

from src.database import get_boto3_session
from src.home.domain.repository import IHomeRepository


class HomeRepository(IHomeRepository):
    def __init__(self):
        self.session = get_boto3_session()

    async def download_from_s3(self, session: boto3.Session, scenario_id: str):
        # env = os.getenv("ENVIRONMENT")
        # if env == "local":
        #     parquet_path = "samples/sim_pax.parquet"
        #     sample_data = os.path.join(os.getcwd(), parquet_path)
        #     df = pd.read_parquet(sample_data)
        # elif env == "dev":
        #     df = wr.s3.read_parquet(
        #         path=f"{S3_SAVE_PATH}/dev/{scenario_id}.parquet", boto3_session=session
        #     )

        df = wr.s3.read_parquet(
            path=f"s3://flexa-dev-ap-northeast-2-data-storage/simulations/simulation-results-raw-data/{scenario_id}.parquet",
            boto3_session=session,
        )

        return df
