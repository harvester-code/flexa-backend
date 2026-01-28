from dependency_injector import containers, providers


from app.routes.ai_agent.application.service import AIAgentService
from app.routes.ai_agent.application.core import CommandExecutor, CommandParser
from app.routes.home.application.service import HomeService
from app.routes.home.infra.repository import HomeRepository
from app.routes.simulation.application.service import SimulationService
from app.routes.simulation.infra.repository import SimulationRepository
from packages.aws.s3.s3_manager import S3Manager


class Container(containers.DeclarativeContainer):
    wiring_config = containers.WiringConfiguration(
        packages=[
            "app.routes.ai_agent",
            "app.routes.home",
            "app.routes.simulation",
            "packages.supabase",
        ]
    )

    s3_manager = providers.Singleton(S3Manager)

    simulation_repo = providers.Factory(SimulationRepository)
    simulation_service = providers.Factory(
        SimulationService, simulation_repo=simulation_repo, s3_manager=s3_manager
    )

    home_repo = providers.Factory(HomeRepository, s3_manager=s3_manager)
    home_service = providers.Factory(HomeService, home_repo=home_repo)

    ai_agent_service = providers.Factory(AIAgentService)
    
    # Command execution dependencies
    command_executor = providers.Factory(
        CommandExecutor,
        simulation_service=simulation_service
    )
    command_parser = providers.Factory(
        CommandParser,
        command_executor=command_executor
    )