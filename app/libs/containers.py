from dependency_injector import containers, providers

from app.routes.admin.application.service import AdminService
from app.routes.admin.infra.repository import AdminRepository
from app.routes.facility.application.service import FacilityService
from app.routes.facility.infra.repository import FacilityRepository
from app.routes.home.application.service import HomeService
from app.routes.home.infra.repository import HomeRepository
from app.routes.simulation.application.service import SimulationService
from app.routes.simulation.infra.repository import SimulationRepository
from packages.aws.s3.s3_downloader import S3Downloader


class Container(containers.DeclarativeContainer):
    wiring_config = containers.WiringConfiguration(
        packages=[
            "app.routes.admin",
            "app.routes.facility",
            "app.routes.home",
            "app.routes.simulation",
        ]
    )

    # S3 Downloader를 싱글톤으로 관리
    s3_downloader = providers.Singleton(S3Downloader)

    simulation_repo = providers.Factory(SimulationRepository)
    simulation_service = providers.Factory(
        SimulationService, simulation_repo=simulation_repo
    )

    home_repo = providers.Factory(HomeRepository, s3_downloader=s3_downloader)
    home_service = providers.Factory(HomeService, home_repo=home_repo)

    facility_repo = providers.Factory(FacilityRepository)
    facility_service = providers.Factory(FacilityService, facility_repo=facility_repo)

    admin_repo = providers.Factory(AdminRepository)
    admin_service = providers.Factory(AdminService, admin_repo=admin_repo)
