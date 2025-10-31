from dependency_injector import containers, providers


from app.routes.home.application.service import HomeService
from app.routes.home.infra.repository import HomeRepository
from app.routes.simulation.application.service import SimulationService
from app.routes.simulation.infra.repository import SimulationRepository
from packages.aws.s3.s3_manager import S3Manager


class Container(containers.DeclarativeContainer):
    wiring_config = containers.WiringConfiguration(
        packages=[
            "app.routes.home",
            "app.routes.simulation",
            "packages.supabase",  # 🔧 Supabase 패키지 추가
        ]
    )

    # 통합 S3Manager를 싱글톤으로 관리
    s3_manager = providers.Singleton(S3Manager)

    simulation_repo = providers.Factory(SimulationRepository)
    simulation_service = providers.Factory(
        SimulationService, simulation_repo=simulation_repo
    )

    home_repo = providers.Factory(HomeRepository, s3_manager=s3_manager)
    home_service = providers.Factory(HomeService, home_repo=home_repo, s3_manager=s3_manager)
