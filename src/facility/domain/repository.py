from abc import ABCMeta, abstractmethod


class IFacilityRepository(metaclass=ABCMeta):
    """
    detailed_facilities
    """

    @abstractmethod
    def download_from_s3(self):
        raise NotImplementedError
