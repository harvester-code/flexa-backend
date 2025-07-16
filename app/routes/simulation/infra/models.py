from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column
from typing import List

from packages.database import Base


class ScenarioInformation(Base):
    __tablename__ = "scenario_information"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    user_id: Mapped[UUID] = mapped_column(UUID, nullable=False)
    editor: Mapped[str] = mapped_column(String(36), nullable=False)
    name: Mapped[str] = mapped_column(String(50), nullable=False)
    terminal: Mapped[str] = mapped_column(String(36), nullable=False)
    airport: Mapped[str] = mapped_column(String(36), nullable=True)
    memo: Mapped[str] = mapped_column(String(200), nullable=True)
    target_flight_schedule_date: Mapped[datetime] = mapped_column(
        DateTime, nullable=True
    )
    status: Mapped[str] = mapped_column(String(10), nullable=True, default="yet")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    simulation_start_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    simulation_end_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class ScenarioMetadata(Base):
    __tablename__ = "scenario_metadata"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    scenario_id: Mapped[str] = mapped_column(String(36), nullable=False)
    overview: Mapped[dict] = mapped_column(JSONB, nullable=True)
    history: Mapped[List[dict]] = mapped_column(JSONB, nullable=True)
    flight_schedule: Mapped[dict] = mapped_column(JSONB, nullable=True)
    passenger_schedule: Mapped[dict] = mapped_column(JSONB, nullable=True)
    processing_procedures: Mapped[dict] = mapped_column(JSONB, nullable=True)
    facility_connection: Mapped[dict] = mapped_column(JSONB, nullable=True)
    facility_information: Mapped[dict] = mapped_column(JSONB, nullable=True)


class Group(Base):
    __tablename__ = "group"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(36), nullable=False)
    description: Mapped[str] = mapped_column(String(36), nullable=True)
    master_scenario_id: Mapped[str] = mapped_column(String(36), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class OperationSetting(Base):
    __tablename__ = "operation_setting"
    __table_args__ = {"extend_existing": True}

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    group_id: Mapped[Integer] = mapped_column(Integer, nullable=False)
    terminal_name: Mapped[str] = mapped_column(String(36), nullable=False)
    terminal_process: Mapped[dict] = mapped_column(JSONB, nullable=True)
    processing_procedure: Mapped[dict] = mapped_column(JSONB, nullable=True)
    terminal_layout: Mapped[dict] = mapped_column(JSONB, nullable=True)
    terminal_layout_image_url: Mapped[str] = mapped_column(String(36), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class UserInformation(Base):
    __tablename__ = "user_information"
    __table_args__ = {"extend_existing": True}

    user_id: Mapped[str] = mapped_column(UUID, primary_key=True)
    email: Mapped[str] = mapped_column(String, nullable=False)
    first_name: Mapped[str] = mapped_column(String, nullable=False)
    last_name: Mapped[str] = mapped_column(String, nullable=False)
    group_id: Mapped[int] = mapped_column(Integer, nullable=True)
    role_id: Mapped[int] = mapped_column(Integer, nullable=True)
    position: Mapped[str] = mapped_column(String, nullable=True)
    bio: Mapped[str] = mapped_column(String, nullable=True)
    profile_image_url: Mapped[str] = mapped_column(String, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
