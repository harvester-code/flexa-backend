from dataclasses import dataclass
from datetime import datetime


@dataclass
class Certification:
    id: str
    email: str
    cert_number: int
    expired_at: datetime
    created_at: datetime


@dataclass
class UserAccessRequest:
    id: str
    user_email: str
    admin_email: str
    request_mg: str
    is_checked: bool
    created_at: datetime
