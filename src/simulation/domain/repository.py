from abc import ABCMeta, abstractmethod
from src.simulation.domain.simulation import ScenarioMetadata, SimulationScenario


class ISimulationRepository(metaclass=ABCMeta):
    """
    시뮬레이션
    """

    """
    시나리오의 전체적인 생성, 로드, 저장, 분석이동
    """

    # 최초 시뮬레이션 생성시 : 1. simulation_management에 로우 생성 + simulation_metadata에 시나리오 히스토리 생성
    # @abstractmethod
    # def create_sim_scenario(
    #     self, metadata: SimulationMetadata, scenario: SimulationScenario
    # ):
    #     raise NotImplementedError

    # # 메타데이터는 현재 필터값만 존재하지만, overview쪽을 일단 첫번째로 보내고 다음으로 나머지를 일단 보낸다. 더 쪼개기는 추후에 진행
    # @abstractmethod
    # def fetch_sim_scenario_overview(self, metadata: SimulationMetadata):
    #     raise NotImplementedError

    # @abstractmethod
    # def fetch_sim_scenario_metadata(self, metadata: SimulationMetadata):
    #     raise NotImplementedError

    # # 시나리오 저장
    # @abstractmethod
    # def update_sim_scenario(self, metadata: SimulationMetadata):
    #     raise NotImplementedError

    # 생성된 시뮬레이션에서 액션버튼을 눌러 분석페이지로 넘어갈때
    # 미정

    """
    각 단계별 디비와 연동되는 매서드
    """

    @abstractmethod
    def create_simulation_scenario(self):
        raise NotImplementedError

    @abstractmethod
    def fetch_simulation_scenario(self):
        raise NotImplementedError

    @abstractmethod
    def fetch_scenario_metadata(self):
        raise NotImplementedError

    @abstractmethod
    def update_scenario_metadata(self):
        raise NotImplementedError

    # Flight Schedule
    @abstractmethod
    def fetch_flight_schedule_data(self):
        raise NotImplementedError

    @abstractmethod
    def update_simulation_scenario(self):
        raise NotImplementedError
