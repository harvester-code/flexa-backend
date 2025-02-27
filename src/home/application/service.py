from dependency_injector.wiring import inject
from src.home.domain.repository import IHomeRepository


class HomeService:
    """
    //매서드 정의//

    """

    @inject
    def __init__(
        self,
        home_repo: IHomeRepository,
    ):
        self.home_repo = home_repo
