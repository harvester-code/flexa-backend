from abc import ABCMeta, abstractmethod


class IAdminRepository(metaclass=ABCMeta):
    """
    admins
    """

    @abstractmethod
    def download_from_s3(self):
        raise NotImplementedError
