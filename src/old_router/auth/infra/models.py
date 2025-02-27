from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.database import Base


class Certification(Base):
    __tablename__ = "certification"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    email: Mapped[str] = mapped_column(String(36), nullable=False)
    cert_number: Mapped[int] = mapped_column(Integer, nullable=False)
    expired_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class UserAccessRequest(Base):
    __tablename__ = "user_access_request"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    user_email: Mapped[str] = mapped_column(String(36), nullable=False)
    admin_email: Mapped[str] = mapped_column(String(36), nullable=False)
    request_mg: Mapped[str] = mapped_column(String(100), nullable=True)
    is_checked: Mapped[Boolean] = mapped_column(Boolean, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
