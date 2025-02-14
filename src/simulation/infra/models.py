from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.database import Base


class SimulationScenario(Base):
    __tablename__ = "simulation_scenario"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    user_id: Mapped[UUID] = mapped_column(UUID, nullable=False)
    simulation_url: Mapped[str] = mapped_column(String(36), nullable=True)
    simulation_name: Mapped[str] = mapped_column(String(36), nullable=False)
    size: Mapped[Integer] = mapped_column(Integer, nullable=True)
    terminal: Mapped[str] = mapped_column(String(36), nullable=False)
    editor: Mapped[str] = mapped_column(String(36), nullable=False)
    memo: Mapped[str] = mapped_column(String(36), nullable=True)
    simulation_date: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class ScenarioMetadata(Base):
    __tablename__ = "scenario_metadata"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    simulation_id: Mapped[str] = mapped_column(String(36), nullable=False)
    overview: Mapped[dict] = mapped_column(JSONB, nullable=True)
    history: Mapped[dict] = mapped_column(JSONB, nullable=True)
    flight_sch: Mapped[dict] = mapped_column(JSONB, nullable=True)
    passenger_sch: Mapped[dict] = mapped_column(JSONB, nullable=True)
    passenger_attr: Mapped[dict] = mapped_column(JSONB, nullable=True)
    facility_conn: Mapped[dict] = mapped_column(JSONB, nullable=True)
    facility_info: Mapped[dict] = mapped_column(JSONB, nullable=True)
