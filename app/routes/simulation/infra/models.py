from datetime import datetime
from typing import List, Optional

from sqlalchemy import Boolean, DateTime, Integer, String, BigInteger, Text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from packages.supabase.database import Base


class ScenarioInformation(Base):
    __tablename__ = "scenario_information"

    id: Mapped[Optional[int]] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True  # bigint
    )
    scenario_id: Mapped[str] = mapped_column(Text, nullable=True)  # text
    user_id: Mapped[UUID] = mapped_column(UUID, nullable=False)
    editor: Mapped[str] = mapped_column(String, nullable=True)   # character varying - NULL 허용으로 변경
    name: Mapped[str] = mapped_column(String, nullable=False)     # character varying
    terminal: Mapped[str] = mapped_column(String, nullable=True)  # character varying - NULL 허용으로 변경
    airport: Mapped[str] = mapped_column(String, nullable=True)   # character varying
    memo: Mapped[str] = mapped_column(Text, nullable=True)        # text
    target_flight_schedule_date: Mapped[str] = mapped_column(     # character varying
        String, nullable=True
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    simulation_start_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    metadata_updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    simulation_status: Mapped[str] = mapped_column(
        String(20), nullable=True, default='pending'
    )
    simulation_error: Mapped[str] = mapped_column(
        Text, nullable=True
    )
    simulation_end_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=True
    )







class UserInformation(Base):
    __tablename__ = "user_information"
    __table_args__ = {"extend_existing": True}

    user_id: Mapped[str] = mapped_column(UUID, primary_key=True)
    email: Mapped[str] = mapped_column(String, nullable=False)
    first_name: Mapped[str] = mapped_column(String, nullable=False)
    last_name: Mapped[str] = mapped_column(String, nullable=False)
    profile_image_url: Mapped[str] = mapped_column(String, nullable=True)
    position: Mapped[str] = mapped_column(String, nullable=True)
    introduction: Mapped[str] = mapped_column(Text, nullable=True)  # bio → introduction
    timezone: Mapped[str] = mapped_column(String, nullable=False)     # 데이터베이스에 존재함
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
