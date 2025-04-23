from abc import ABCMeta, abstractmethod

import boto3


class IHomeRepository(metaclass=ABCMeta):
    """
    홈 화면
    """

    @abstractmethod
    async def download_from_s3(self, session: boto3.Session, scenario_id: str):
        raise NotImplementedError
