from dependency_injector import containers, providers

# from src.auth.application.service import AuthService
# from src.auth.infra.repository import AuthRepository

from src.simulation.application.service import SimulationService
from src.simulation.infra.repository import SimulationRepository

from src.home.application.service import HomeService
from src.home.infra.repository import HomeRepository


class Container(containers.DeclarativeContainer):
    wiring_config = containers.WiringConfiguration(
        packages=["src.simulation", "src.home"],  # "src.auth",
    )

    # auth_repo = providers.Factory(AuthRepository)
    # auth_service = providers.Factory(AuthService, auth_repo=auth_repo)

    simulation_repo = providers.Factory(SimulationRepository)
    simulation_service = providers.Factory(
        SimulationService, simulation_repo=simulation_repo
    )

    home_repo = providers.Factory(HomeRepository)
    home_service = providers.Factory(HomeService, home_repo=home_repo)
