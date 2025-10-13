from __future__ import annotations

import json
import math
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional, Union

AircraftMetadata = Dict[str, Optional[str]]


@lru_cache(maxsize=1)
def _load_aircraft_type_mapping() -> Dict[str, AircraftMetadata]:
    """Load the IATA aircraft code to aircraft metadata mapping."""
    mapping_path = Path(__file__).with_name("aircraft_types.json")
    with mapping_path.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    if not isinstance(data, dict):
        raise ValueError("aircraft_types.json must contain an object mapping codes to metadata")

    normalized: Dict[str, AircraftMetadata] = {}
    for key, value in data.items():
        if not isinstance(key, str) or not isinstance(value, dict):
            continue

        code = key.strip().upper()
        manufacturer = value.get("manufacturer") if isinstance(value, dict) else None
        name = value.get("name") if isinstance(value, dict) else None
        cabin_class = value.get("class") if isinstance(value, dict) else None

        normalized[code] = {
            "code": code,
            "manufacturer": manufacturer.strip() if isinstance(manufacturer, str) else None,
            "name": name.strip() if isinstance(name, str) else None,
            "class": cabin_class.strip() if isinstance(cabin_class, str) else None,
        }

    return normalized


def _normalize_code(iata_code: Optional[Union[str, int, float]]) -> Optional[str]:
    if iata_code is None:
        return None

    if isinstance(iata_code, float):
        if math.isnan(iata_code):
            return None
        if iata_code.is_integer():
            iata_code = str(int(iata_code))
        else:
            iata_code = str(iata_code)
    elif isinstance(iata_code, int):
        iata_code = str(iata_code)
    elif not isinstance(iata_code, str):
        iata_code = str(iata_code)

    code = iata_code.strip().upper()
    if not code or code in {"NAN", "NONE"}:
        return None

    return code


def get_aircraft_metadata(iata_code: Optional[Union[str, int, float]]) -> AircraftMetadata:
    """Return detailed metadata (code, manufacturer, name, class) for an IATA aircraft code."""
    code_key = _normalize_code(iata_code)
    if code_key is None:
        return {"code": None, "manufacturer": None, "name": None, "class": None}

    mapping = _load_aircraft_type_mapping()
    entry = mapping.get(code_key)
    if entry is None:
        return {"code": code_key, "manufacturer": None, "name": None, "class": None}
    return dict(entry)


def get_aircraft_name(iata_code: Optional[Union[str, int, float]]) -> str:
    """Return a human-friendly aircraft name for the given IATA code."""
    metadata = get_aircraft_metadata(iata_code)
    name = metadata.get("name")
    code = metadata.get("code")
    if name:
        return name
    if code:
        return code
    return "Unknown"


def get_aircraft_class(iata_code: Optional[Union[str, int, float]]) -> str:
    """Return the aircraft class (e.g., A/B/C) for the given IATA code."""
    metadata = get_aircraft_metadata(iata_code)
    aircraft_class = metadata.get("class")
    if aircraft_class:
        return aircraft_class
    return "Unknown"
