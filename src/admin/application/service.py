from dependency_injector.wiring import inject
from src.admin.domain.repository import IAdminRepository
import boto3
import pandas as pd
import numpy as np


class AdminService:
    """
    //매서드 정의//

    """

    @inject
    def __init__(
        self,
        admin_repo: IAdminRepository,
    ):
        self.admin_repo = admin_repo

    async def test(self, session: boto3.Session, process):

        data = pd.read_csv("samples/test_sample.csv")
