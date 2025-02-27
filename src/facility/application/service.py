from dependency_injector.wiring import inject
from src.facility.domain.repository import IFacilityRepository


class FacilityService:
    """
    //매서드 정의//

    """

    @inject
    def __init__(
        self,
        facility_repo: IFacilityRepository,
    ):
        self.facility_repo = facility_repo
