from abc import ABCMeta, abstractmethod


class IAdminRepository(metaclass=ABCMeta):
    """
    admins
    """

    @abstractmethod
    def fetch_operation_setting(self):
        raise NotImplementedError

    @abstractmethod
    def create_operation_setting(self):
        raise NotImplementedError

    @abstractmethod
    def update_operation_setting(self):
        raise NotImplementedError
