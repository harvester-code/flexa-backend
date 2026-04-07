"""
OAG 공항 코드 레퍼런스 (packages/oag_ref.xlsx — Airport Code 시트)

Snowflake MASTER_LOCATION 대신 사용. IATA 3글자 공항 코드로 도시/국가/지역을 매칭.
Eff From / Eff To 유효기간 처리는 항공사 레퍼런스와 동일한 2자리 윈도우 보정 적용.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional

import pandas as pd
from loguru import logger

from .oag_airline_reference import _cell_to_date, _default_xlsx_path, _parse_iata

AIRPORT_CODE_SHEET = "Airport Code"


class AirportInfo(NamedTuple):
    city: str
    country: str
    region: str
    eff_from: date
    eff_to: date


def _load_records(path: Optional[Path] = None) -> Dict[str, List[AirportInfo]]:
    xlsx = path or _default_xlsx_path()
    if not xlsx.is_file():
        logger.warning(f"OAG airport reference missing: {xlsx}")
        return {}

    df = pd.read_excel(xlsx, sheet_name=AIRPORT_CODE_SHEET)
    need = {"IATA", "City Name", "Country Name", "Region Name", "Eff From", "Eff To"}
    missing = need - set(df.columns)
    if missing:
        logger.error(f"OAG airport sheet missing columns {missing}: {xlsx}")
        return {}

    by_code: Dict[str, List[AirportInfo]] = {}
    for _, r in df.iterrows():
        iata = _parse_iata(r.get("IATA"))
        if not iata or len(iata) != 3:
            continue

        city = str(r.get("City Name", "") or "").strip()
        country = str(r.get("Country Name", "") or "").strip()
        region = str(r.get("Region Name", "") or "").strip()
        if not city and not country:
            continue

        eff_from = _cell_to_date(r.get("Eff From"))
        if eff_from is None:
            continue
        eff_to = _cell_to_date(r.get("Eff To"))
        if eff_to is None:
            continue

        by_code.setdefault(iata, []).append(
            AirportInfo(city, country, region, eff_from, eff_to)
        )

    logger.info(
        f"OAG airport reference loaded: {sum(len(v) for v in by_code.values())} rows, "
        f"{len(by_code)} unique airports from {xlsx.name}"
    )
    return by_code


_records_cache: Optional[Dict[str, List[AirportInfo]]] = None
_cache_path: Optional[str] = None


def _get_records(path: Optional[Path] = None) -> Dict[str, List[AirportInfo]]:
    global _records_cache, _cache_path
    resolved = path or _default_xlsx_path()
    key = str(resolved.resolve())
    if _records_cache is None or _cache_path != key:
        _records_cache = _load_records(resolved)
        _cache_path = key
    return _records_cache


class AirportLookupResult(NamedTuple):
    city: str
    country: str
    region: str


def lookup_airport(
    iata: Optional[str],
    as_of: date,
    *,
    path: Optional[Path] = None,
) -> Optional[AirportLookupResult]:
    """
    IATA 3글자 공항 코드와 기준일 as_of에 유효한 도시/국가/지역을 반환.
    여러 행이 겹치면 Eff From이 가장 늦은 행을 사용.
    """
    if not iata:
        return None
    code = str(iata).strip().upper()
    if not code or len(code) != 3:
        return None

    entries = _get_records(path).get(code)
    if not entries:
        return None

    best: Optional[AirportInfo] = None
    for info in entries:
        if info.eff_from > as_of:
            continue
        if as_of > info.eff_to:
            continue
        if best is None or info.eff_from > best.eff_from:
            best = info

    if best is None:
        return None
    return AirportLookupResult(best.city, best.country, best.region)
