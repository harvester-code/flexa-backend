from dependency_injector import containers, providers

from src.auth.application.service import AuthService
from src.auth.infra.repository import AuthRepository


class Container(containers.DeclarativeContainer):
    wiring_config = containers.WiringConfiguration(
        packages=["src.auth"],
    )

    auth_repo = providers.Factory(AuthRepository)
    auth_service = providers.Factory(AuthService, auth_repo=auth_repo)
