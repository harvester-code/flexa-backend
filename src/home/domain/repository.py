from abc import ABCMeta, abstractmethod
from fastapi import Request


class IHomeRepository(metaclass=ABCMeta):
    """
    홈 화면
    """

    @abstractmethod
    async def login_supabase(self, email: str, password: str):
        raise NotImplementedError

    @abstractmethod
    async def fetch_supabase_data(self):
        raise NotImplementedError

    @abstractmethod
    async def fetch_simulation_files(self):
        raise NotImplementedError

    @abstractmethod
    async def fetch_simulation_summary(self, file_id: str):
        raise NotImplementedError
