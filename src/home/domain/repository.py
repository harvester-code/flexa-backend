from abc import ABCMeta, abstractmethod


class IHomeRepository(metaclass=ABCMeta):
    """
    홈 화면
    """

    @abstractmethod
    def fetch_supabase_data(self):
        raise NotImplementedError

    @abstractmethod
    def fetch_simulation_files(self):
        raise NotImplementedError

    @abstractmethod
    def fetch_simulation_summary(self, file_id: str):
        raise NotImplementedError
