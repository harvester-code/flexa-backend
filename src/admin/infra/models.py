from sqlalchemy import Integer, String
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
