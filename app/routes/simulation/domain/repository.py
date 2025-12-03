from abc import ABCMeta, abstractmethod


class ISimulationRepository(metaclass=ABCMeta):
    """
    시뮬레이션
    """

    # ===================================
    # NOTE: 시뮬레이션 시나리오

    @abstractmethod
    def fetch_scenario_information(self):
        raise NotImplementedError

    @abstractmethod
    def create_scenario_information(self):
        raise NotImplementedError

    @abstractmethod
    def update_scenario_information(self):
        raise NotImplementedError

    @abstractmethod
    def update_metadata_updated_at(self):
        raise NotImplementedError

    @abstractmethod
    def update_simulation_start_at(self):
        raise NotImplementedError

    @abstractmethod
    def deactivate_scenario_information(self):
        raise NotImplementedError

    # ===================================
    # NOTE: 시뮬레이션 프로세스

    @abstractmethod
    def update_scenario_target_flight_schedule_date(self):
        raise NotImplementedError

    @abstractmethod
    def check_user_scenario_permission(self):
        raise NotImplementedError
