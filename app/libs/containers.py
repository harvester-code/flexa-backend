from dependency_injector import containers, providers


from app.routes.home.application.service import HomeService
from app.routes.home.infra.repository import HomeRepository
from app.routes.simulation.application.service import SimulationService
from app.routes.simulation.infra.repository import SimulationRepository
from packages.aws.s3.s3_downloader import S3Downloader


class Container(containers.DeclarativeContainer):
    wiring_config = containers.WiringConfiguration(
        packages=[
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
