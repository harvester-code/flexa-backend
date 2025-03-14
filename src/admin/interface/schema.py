from typing import Any, Dict, List

from pydantic import BaseModel


class CreateOperationSettingBody(BaseModel):
    terminal_name: str


class UpdateOperationSettingBody(BaseModel):
    terminal_name: str | None
    terminal_process: dict | None
    processing_procedure: dict | None
    terminal_layout: dict | None
    terminal_layout_image_url: str | None
