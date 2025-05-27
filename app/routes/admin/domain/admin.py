from dataclasses import dataclass


@dataclass
class OperationSetting:
    id: str
    group_id: str
    terminal_name: str
    terminal_process: dict | None
    processing_procedure: dict | None
    terminal_layout: dict | None
    terminal_layout_image_url: str | None
