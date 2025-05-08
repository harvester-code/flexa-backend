from abc import ABCMeta, abstractmethod


class IHomeRepository(metaclass=ABCMeta):
    """
    홈 화면
    """

    @abstractmethod
    async def download_from_s3(self, scenario_id: str):
        raise NotImplementedError
