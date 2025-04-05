from abc import ABCMeta, abstractmethod
from src.auth.domain.auth import Certification, UserAccessRequest


class IAuthRepository(metaclass=ABCMeta):

    @abstractmethod
    def create_certification(self, certification: Certification):
        raise NotImplementedError

    @abstractmethod
    def fetch_certification(self, certification: Certification):
        raise NotImplementedError

    @abstractmethod
    def create_user(self):
        raise NotImplementedError

    @abstractmethod
    def create_user_access_request(self, user_access_request: UserAccessRequest):
        raise NotImplementedError

    @abstractmethod
    def login_user(self):
        raise NotImplementedError

    @abstractmethod
    def logout_user(self):
        raise NotImplementedError

    @abstractmethod
    def reset_password(self):
        raise NotImplementedError
