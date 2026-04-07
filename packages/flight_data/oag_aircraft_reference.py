"""
OAG 항공기 코드 레퍼런스 (packages/oag_ref.xlsx — Aircraft Code 시트)

IATA 항공기 코드(예: 738, 77W, 320)로 기종명(Acft Name)을 매칭.
Eff From / Eff To 유효기간 처리는 항공사/공항 레퍼런스와 동일한 2자리 윈도우 보정 적용.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional

import pandas as pd
from loguru import logger

from .oag_airline_reference import _cell_to_date, _default_xlsx_path, _parse_iata

AIRCRAFT_CODE_SHEET = "Aircraft Code"


class AircraftInfo(NamedTuple):
    name: str
    eff_from: date
    eff_to: date


def _load_records(path: Optional[Path] = None) -> Dict[str, List[AircraftInfo]]:
    xlsx = path or _default_xlsx_path()
    if not xlsx.is_file():
        logger.warning(f"OAG aircraft reference missing: {xlsx}")
        return {}

    df = pd.read_excel(xlsx, sheet_name=AIRCRAFT_CODE_SHEET)
    need = {"IATA", "Acft Name", "Eff From", "Eff To"}
    missing = need - set(df.columns)
    if missing:
        logger.error(f"OAG aircraft sheet missing columns {missing}: {xlsx}")
        return {}

    by_code: Dict[str, List[AircraftInfo]] = {}
    for _, r in df.iterrows():
        iata = _parse_iata(r.get("IATA"))
        if not iata:
            continue

        name = str(r.get("Acft Name", "") or "").strip()
        if not name:
            continue

        eff_from = _cell_to_date(r.get("Eff From"))
        if eff_from is None:
            continue
        eff_to = _cell_to_date(r.get("Eff To"))
        if eff_to is None:
            continue

        by_code.setdefault(iata, []).append(
            AircraftInfo(name, eff_from, eff_to)
        )

    logger.info(
        f"OAG aircraft reference loaded: {sum(len(v) for v in by_code.values())} rows, "
        f"{len(by_code)} unique codes from {xlsx.name}"
    )
    return by_code


_records_cache: Optional[Dict[str, List[AircraftInfo]]] = None
_cache_path: Optional[str] = None


def _get_records(path: Optional[Path] = None) -> Dict[str, List[AircraftInfo]]:
    global _records_cache, _cache_path
    resolved = path or _default_xlsx_path()
    key = str(resolved.resolve())
    if _records_cache is None or _cache_path != key:
        _records_cache = _load_records(resolved)
        _cache_path = key
    return _records_cache


def lookup_aircraft_name(
    iata: Optional[str],
    as_of: date,
    *,
    path: Optional[Path] = None,
) -> Optional[str]:
    """
    IATA 항공기 코드와 기준일 as_of에 유효한 기종명을 반환.
    여러 행이 겹치면 Eff From이 가장 늦은 행을 사용.
    """
    if not iata:
        return None
    code = str(iata).strip().upper()
    if not code:
        return None

    entries = _get_records(path).get(code)
    if not entries:
        return None

    best: Optional[AircraftInfo] = None
    for info in entries:
        if info.eff_from > as_of:
            continue
        if as_of > info.eff_to:
            continue
        if best is None or info.eff_from > best.eff_from:
            best = info

    return best.name if best else None
