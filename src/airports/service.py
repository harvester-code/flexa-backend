from typing import Annotated, List

from fastapi import Depends
from sqlmodel import Session, col, select

from src.airports.model import GeneralDeclarationDeparture
from src.database import get_snowflake_session

SessionDep = Annotated[Session, Depends(get_snowflake_session)]


class AirportService:
    @staticmethod
    def fetch_general_declarations(
        session: SessionDep,
    ) -> List[GeneralDeclarationDeparture]:

        general_declarations = session.exec(
            select(GeneralDeclarationDeparture)
            .where(col(GeneralDeclarationDeparture.DEPARTURE_AIRPORT_ID) == "ICN")
            .where(col(GeneralDeclarationDeparture.FLIGHT_DATE_UTC) == "2024-12-17")
        ).all()

        return general_declarations

    def show_up(session: SessionDep):
        # =================================
        data = session.exec(
            select(GeneralDeclarationDeparture)
            .where(col(GeneralDeclarationDeparture.DEPARTURE_AIRPORT_ID) == "ICN")
            .where(col(GeneralDeclarationDeparture.FLIGHT_DATE_UTC) == "2024-12-17")
        ).all()
        # =================================

        # =================================
        # NOTE: SHOW UP
        pass
