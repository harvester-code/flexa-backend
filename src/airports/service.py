from typing import Annotated, List

from fastapi import Depends
from sqlmodel import Session, select

from src.airports.model import GeneralDeclaration
from src.database import get_snowflake_session

SessionDep = Annotated[Session, Depends(get_snowflake_session)]


class AirportService:
    @staticmethod
    def fetch_general_declarations(session: SessionDep) -> List[GeneralDeclaration]:
        general_declarations = session.exec(select(GeneralDeclaration).limit(100)).all()
        return general_declarations
