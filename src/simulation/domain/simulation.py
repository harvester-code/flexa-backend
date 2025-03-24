from dataclasses import dataclass
from datetime import datetime
from typing import List


@dataclass
class SimulationScenario:
    id: str
    user_id: str
    simulation_url: str | None
    simulation_name: str
    size: str | None
    terminal: str
    editor: str
    memo: str
    simulation_date: str | None
    created_at: datetime  # 생성날짜? 수정날짜? 수정날짜로 일단 생각
    updated_at: datetime


@dataclass
class ScenarioMetadata:
    scenario_id: str
    # 여기서 dict는 jsonb 형태로 supabase에 저장될 예정
    overview: dict | None
    history: List[dict] | None
    flight_sch: dict | None
    passenger_sch: dict | None
    passenger_attr: dict | None
    facility_conn: dict | None
    facility_info: dict | None


overview = {
    "date": "",
    "terminal": "",
    "analysis_type": "",
    "source": "",
    "flights": "",
    "passengers": "",
    "passengers_pattern": "",
    "generation_method": "",
    "process_1": "",
    "process_2": "",
    "process_3": "",
    "process_4": "",
}

history = [
    {
        "checkpoint": "",
        "modification_date": "",
        "simulation": "",
        "memo": "",
        "error": "",
    }
]

flight_sch = {
    "date": "",
    "airport": "",
    "condition": [
        {"criteria": "Airline", "operator": "is in", "value": ["OZ", "KE"]},
        {"criteria": "Airline", "operator": "is in", "value": ["OZ", "KE"]},
        {"criteria": "Airline", "operator": "is in", "value": ["OZ", "KE"]},
    ],
}

passenger_sch = [
    {
        "index": 0,
        "conditions": [
            {"criteria": "Airline", "operator": "is in", "value": ["OZ", "KE"]}
        ],
        "mean": 120,
        "standard_deviation": 40,
    },
    {"index": 9999, "conditions": [], "mean": 150, "standard_deviation": 50},
]

passenger_attr = {
    "name": "PRM_STATUS",
    "rows": "operating_carrier_name",
    "columns": ["yes", "no"],
    "table": {
        "index": ["Asiana_airline", "jinair", ...],
        "columns": ["yes", "no"],
        "values": [[0, 1, ...], [1, 0, ...]],
    },
}

facility_conn = {
    "processes": {
        "0": {
            "name": "operating_carrier_name",
            "nodes": [],
            "source": None,
            "destination": "1",
            "wait_time": None,
            "default_matrix": None,
            "priority_matrix": None,
        },
        "1": {
            "name": "checkin",
            "nodes": ["A", "B", "C", "D"],
            "source": "0",
            "destination": "2",
            "wait_time": 10,
            "default_matrix": {
                "Asiana Airlines": {"A": 0.5, "B": 0.5, "C": 0.0, "D": 0.0},
                "T'way Air": {"A": 0.0, "B": 0.0, "C": 0.5, "D": 0.5},
            },
            "priority_matrix": [
                {
                    "condition": [
                        {
                            "criteria": "Airline",
                            "operator": "is in",
                            "value": ["OZ", "KE"],
                        }
                    ],
                    "matrix": {
                        "Asiana Airlines": {"A": 0.5, "B": 0.5, "C": 0.0, "D": 0.0},
                        "Korean Air": {"A": 0.0, "B": 0.0, "C": 0.5, "D": 0.5},
                    },
                }
            ],
        },
        "2": {
            "name": "departure_gate",
            "nodes": ["DG1", "DG2"],
            "source": "1",
            "destination": "3",
            "wait_time": 10,
            "default_matrix": {
                "A": {"DG1": 0.5, "DG2": 0.5},
                "B": {"DG1": 0.5, "DG2": 0.5},
                "C": {"DG1": 0.5, "DG2": 0.5},
                "D": {"DG1": 0.5, "DG2": 0.5},
            },
            "priority_matrix": [
                {
                    "condition": [
                        {
                            "criteria": "Airline",
                            "operator": "is in",
                            "value": ["OZ", "KE"],
                        }
                    ],
                    "matrix": {
                        "A": {"DG1": 0.5, "DG2": 0.5},
                        "B": {"DG1": 1.0, "DG2": 0.0},
                    },
                }
            ],
        },
        "3": {
            "name": "security",
            "nodes": ["SC1", "SC2"],
            "source": "2",
            "destination": "4",
            "wait_time": 10,
            "default_matrix": {
                "DG1": {"SC1": 0.5, "SC2": 0.5},
                "DG2": {"SC1": 0.5, "SC2": 0.5},
            },
            "priority_matrix": [
                {
                    "condition": [
                        {
                            "criteria": "Airline",
                            "operator": "is in",
                            "value": ["OZ", "KE"],
                        }
                    ],
                    "matrix": {
                        "DG1": {"SC1": 0.5, "SC2": 0.5},
                        "DG2": {"SC1": 1.0, "SC2": 0.0},
                    },
                }
            ],
        },
        "4": {
            "name": "passport",
            "nodes": ["PC1", "PC2"],
            "source": "3",
            "destination": None,
            "wait_time": 10,
            "default_matrix": {
                "SC1": {"PC1": 0.5, "PC2": 0.5},
                "SC2": {"PC1": 0.5, "PC2": 0.5},
            },
            "priority_matrix": [
                {
                    "condition": [
                        {
                            "criteria": "Airline",
                            "operator": "is in",
                            "value": ["OZ", "KE"],
                        }
                    ],
                    "matrix": {
                        "SC1": {"PC1": 0.5, "PC2": 0.5},
                        "SC2": {"PC1": 1.0, "PC2": 0.0},
                    },
                }
            ],
        },
    },
}

facility_info = {
    "components": [
        {
            "name": "checkin",
            "nodes": [
                {
                    "id": 0,
                    "name": "A",
                    "max_queue_length": 250,
                    "facility_count": 6,
                    "facility_schedules": [
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                    ],
                },
                {
                    "id": 1,
                    "name": "B",
                    "max_queue_length": 250,
                    "facility_count": 6,
                    "facility_schedules": [
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                    ],
                },
                {
                    "id": 2,
                    "name": "C",
                    "max_queue_length": 250,
                    "facility_count": 6,
                    "facility_schedules": [
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                    ],
                },
                {
                    "id": 3,
                    "name": "D",
                    "max_queue_length": 250,
                    "facility_count": 6,
                    "facility_schedules": [
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                    ],
                },
            ],
        },
        {
            "name": "departure_gate",
            "nodes": [
                {
                    "id": 4,
                    "name": "DG1",
                    "max_queue_length": 250,
                    "facility_count": 6,
                    "facility_schedules": [
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                    ],
                },
                {
                    "id": 5,
                    "name": "DG2",
                    "max_queue_length": 250,
                    "facility_count": 6,
                    "facility_schedules": [
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                    ],
                },
            ],
        },
        {
            "name": "security",
            "nodes": [
                {
                    "id": 6,
                    "name": "SC1",
                    "max_queue_length": 250,
                    "facility_count": 6,
                    "facility_schedules": [
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                    ],
                },
                {
                    "id": 7,
                    "name": "SC2",
                    "max_queue_length": 250,
                    "facility_count": 6,
                    "facility_schedules": [
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                    ],
                },
            ],
        },
        {
            "name": "passport",
            "nodes": [
                {
                    "id": 8,
                    "name": "PC1",
                    "max_queue_length": 250,
                    "facility_count": 6,
                    "facility_schedules": [
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                    ],
                },
                {
                    "id": 9,
                    "name": "PC2",
                    "max_queue_length": 250,
                    "facility_count": 6,
                    "facility_schedules": [
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                        [60, 60, 60, 60, 60, 60],
                    ],
                },
            ],
        },
    ]
}
