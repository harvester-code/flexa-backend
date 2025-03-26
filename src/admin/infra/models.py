from sqlalchemy import Integer, String, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.database import Base


class OperationSetting(Base):
    __tablename__ = "operation_setting"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    group_id: Mapped[Integer] = mapped_column(Integer, nullable=False)
    terminal_name: Mapped[str] = mapped_column(String(36), nullable=False)
    terminal_process: Mapped[dict] = mapped_column(JSONB, nullable=True)
    processing_procedure: Mapped[dict] = mapped_column(JSONB, nullable=True)
    terminal_layout: Mapped[dict] = mapped_column(JSONB, nullable=True)
    terminal_layout_image_url: Mapped[str] = mapped_column(String(36), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class Groups(Base):
    __tablename__ = "groups"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    master_scenario_id: Mapped[str] = mapped_column(String(36), nullable=True)
    group_name: Mapped[str] = mapped_column(String(36), nullable=False)
    description: Mapped[str] = mapped_column(String(36), nullable=True)
    timezone: Mapped[str] = mapped_column(String(36), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
