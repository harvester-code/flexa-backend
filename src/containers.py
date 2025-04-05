from dependency_injector import containers, providers

from src.simulation.application.service import SimulationService
from src.simulation.infra.repository import SimulationRepository

from src.home.application.service import HomeService
from src.home.infra.repository import HomeRepository

from src.facility.application.service import FacilityService
from src.facility.infra.repository import FacilityRepository

from src.admin.application.service import AdminService
from src.admin.infra.repository import AdminRepository


class Container(containers.DeclarativeContainer):
    wiring_config = containers.WiringConfiguration(
        packages=["src.simulation", "src.home", "src.facility", "src.admin"]
    )

    simulation_repo = providers.Factory(SimulationRepository)
    simulation_service = providers.Factory(
        SimulationService, simulation_repo=simulation_repo
    )

    home_repo = providers.Factory(HomeRepository)
    home_service = providers.Factory(HomeService, home_repo=home_repo)

    facility_repo = providers.Factory(FacilityRepository)
    facility_service = providers.Factory(FacilityService, facility_repo=facility_repo)

    admin_repo = providers.Factory(AdminRepository)
    admin_service = providers.Factory(AdminService, admin_repo=admin_repo)
