from abc import ABCMeta, abstractmethod


class ISimulationRepository(metaclass=ABCMeta):
    """
    시뮬레이션
    """

    # ===================================
    # NOTE: 시뮬레이션 시나리오

    @abstractmethod
    def fetch_simulation_scenario(self):
        raise NotImplementedError

    @abstractmethod
    def fetch_simulation_location(self):
        raise NotImplementedError

    @abstractmethod
    def create_simulation_scenario(self):
        raise NotImplementedError

    @abstractmethod
    def update_simulation_scenario(self):
        raise NotImplementedError

    @abstractmethod
    def deactivate_simulation_scenario(self):
        raise NotImplementedError

    @abstractmethod
    def duplicate_simulation_scenario(self):
        raise NotImplementedError

    @abstractmethod
    def update_master_scenario(self):
        raise NotImplementedError

    # ===================================
    # NOTE: 시뮬레이션 시나리오 메타데이터

    @abstractmethod
    def fetch_scenario_metadata(self):
        raise NotImplementedError

    @abstractmethod
    def update_scenario_metadata(self):
        raise NotImplementedError

    # ===================================
    # NOTE: 시뮬레이션 프로세스

    @abstractmethod
    def fetch_flight_schedule_data(self):
        raise NotImplementedError

    @abstractmethod
    def update_simulation_scenario_target_date(self):
        raise NotImplementedError

    @abstractmethod
    def fetch_processing_procedures(self):
        raise NotImplementedError

    @abstractmethod
    def upsert_scenario_status(self):
        return NotImplementedError
