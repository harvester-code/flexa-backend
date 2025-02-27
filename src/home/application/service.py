from dependency_injector.wiring import inject
from src.home.domain.repository import IHomeRepository


class HomeService:
    """
    //매서드 정의//
    generate : 각 controller의 최상위 매서드 (하위 매서드를 포함)
    run_simulation : 최종 시뮬레이션 코드 (현재 분할 필요)

    _calculate: 새로운 데이터 생성 (내장함수)
    _create : 차트 생성 (내장함수)

    fetch : get
    create : post
    update : patch / put
    deactivate : patch (delete)
    """

    # TODO: 그래프 만드는 코드들이 서로 비슷한데 합칠수는 없을까?
    ...

    @inject
    def __init__(
        self,
        home_repo: IHomeRepository,
    ):
        self.home_repo = home_repo
