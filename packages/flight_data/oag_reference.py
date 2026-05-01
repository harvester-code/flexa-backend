"""
OAG 레퍼런스 (packages/flight_data/oag_ref.xlsx)

Airline Code  시트 → lookup_airline_name(iata, as_of)
Aircraft Code 시트 → lookup_aircraft_name(iata, as_of)
Airport Code  시트 → lookup_airport(iata, as_of)

공통 헬퍼:
  - _fix_century : OAG 엑셀 연도 보정 (2038→1938 등, 2자리 윈도우)
  - _cell_to_date: 엑셀 셀 값 → date
  - _parse_iata  : 원시 값 → 대문자 IATA 코드 (None이면 None)
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Tuple

import pandas as pd
from loguru import logger


# ============================================================
# 공통 헬퍼
# ============================================================

def _default_xlsx_path() -> Path:
    return Path(__file__).resolve().parent / "oag_ref.xlsx"


def _fix_century(d: date) -> date:
    """OAG 엑셀 연도 보정: 끝 2자리 < 70 → 20xx, >= 70 → 19xx."""
    yy = d.year % 100
    century = 2000 if yy < 70 else 1900
    corrected = century + yy
    if corrected == d.year:
        return d
    return d.replace(year=corrected)


def _cell_to_date(val: object) -> Optional[date]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, datetime):
        return _fix_century(val.date())
    if isinstance(val, date):
        return _fix_century(val)
    ts = pd.to_datetime(val, errors="coerce")
    if pd.isna(ts):
        return None
    return _fix_century(ts.date())


def _parse_iata(raw: object) -> Optional[str]:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip().upper()
    if not s or s == "NAN":
        return None
    if len(s) > 3:
        return None
    return s


# ============================================================
# Airline Code
# ============================================================

_AirlineRecord = Tuple[str, str, date, date]  # (iata, name, eff_from, eff_to)

_airline_cache: Optional[List[_AirlineRecord]] = None
_airline_cache_path: Optional[str] = None


def _load_airline_records(path: Optional[Path] = None) -> List[_AirlineRecord]:
    xlsx = path or _default_xlsx_path()
    if not xlsx.is_file():
        logger.warning(f"OAG airline reference missing: {xlsx}")
        return []

    df = pd.read_excel(xlsx, sheet_name="Airline Code")
    need = {"IATA", "Airline Name", "Eff From", "Eff To"}
    missing = need - set(df.columns)
    if missing:
        logger.error(f"OAG airline sheet missing columns {missing}: {xlsx}")
        return []

    rows: List[_AirlineRecord] = []
    for _, r in df.iterrows():
        iata = _parse_iata(r.get("IATA"))
        if not iata:
            continue
        name_raw = r.get("Airline Name")
        if name_raw is None or (isinstance(name_raw, float) and pd.isna(name_raw)):
            continue
        name = str(name_raw).strip()
        if not name:
            continue
        eff_from = _cell_to_date(r.get("Eff From"))
        if eff_from is None:
            continue
        eff_to = _cell_to_date(r.get("Eff To"))
        if eff_to is None:
            continue
        rows.append((iata, name, eff_from, eff_to))

    logger.info(f"OAG airline reference loaded: {len(rows)} rows from {xlsx.name}")
    return rows


def _get_airline_records(path: Optional[Path] = None) -> List[_AirlineRecord]:
    global _airline_cache, _airline_cache_path
    resolved = path or _default_xlsx_path()
    key = str(resolved.resolve())
    if _airline_cache is None or _airline_cache_path != key:
        _airline_cache = _load_airline_records(resolved)
        _airline_cache_path = key
    return _airline_cache


def lookup_airline_name(
    iata: Optional[str],
    as_of: date,
    *,
    path: Optional[Path] = None,
) -> Optional[str]:
    """IATA 코드와 기준일 as_of에 유효한 항공사 표시명을 반환."""
    if not iata:
        return None
    code = str(iata).strip().upper()
    if not code:
        return None

    best: Optional[Tuple[date, str]] = None
    for row_code, name, eff_from, eff_to in _get_airline_records(path):
        if row_code != code:
            continue
        if eff_from > as_of or as_of > eff_to:
            continue
        if best is None or eff_from > best[0]:
            best = (eff_from, name)

    return best[1] if best else None


def parse_flight_date(value: object) -> Optional[date]:
    """스케줄 row의 flight_date를 date로 통일."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "date") and callable(value.date) and not isinstance(value, date):
        try:
            d = value.date()
            if isinstance(d, date):
                return d
        except Exception:
            pass
    if isinstance(value, str):
        s = value.strip()[:10]
        try:
            return date.fromisoformat(s)
        except ValueError:
            return None
    return None


# ============================================================
# Aircraft Code
# ============================================================

class _AircraftInfo(NamedTuple):
    name: str
    eff_from: date
    eff_to: date


_aircraft_cache: Optional[Dict[str, List[_AircraftInfo]]] = None
_aircraft_cache_path: Optional[str] = None


def _load_aircraft_records(path: Optional[Path] = None) -> Dict[str, List[_AircraftInfo]]:
    xlsx = path or _default_xlsx_path()
    if not xlsx.is_file():
        logger.warning(f"OAG aircraft reference missing: {xlsx}")
        return {}

    df = pd.read_excel(xlsx, sheet_name="Aircraft Code")
    need = {"IATA", "Acft Name", "Eff From", "Eff To"}
    missing = need - set(df.columns)
    if missing:
        logger.error(f"OAG aircraft sheet missing columns {missing}: {xlsx}")
        return {}

    by_code: Dict[str, List[_AircraftInfo]] = {}
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
        by_code.setdefault(iata, []).append(_AircraftInfo(name, eff_from, eff_to))

    logger.info(
        f"OAG aircraft reference loaded: {sum(len(v) for v in by_code.values())} rows, "
        f"{len(by_code)} unique codes from {xlsx.name}"
    )
    return by_code


def _get_aircraft_records(path: Optional[Path] = None) -> Dict[str, List[_AircraftInfo]]:
    global _aircraft_cache, _aircraft_cache_path
    resolved = path or _default_xlsx_path()
    key = str(resolved.resolve())
    if _aircraft_cache is None or _aircraft_cache_path != key:
        _aircraft_cache = _load_aircraft_records(resolved)
        _aircraft_cache_path = key
    return _aircraft_cache


def lookup_aircraft_name(
    iata: Optional[str],
    as_of: date,
    *,
    path: Optional[Path] = None,
) -> Optional[str]:
    """IATA 항공기 코드와 기준일 as_of에 유효한 기종명을 반환."""
    if not iata:
        return None
    code = str(iata).strip().upper()
    if not code:
        return None

    entries = _get_aircraft_records(path).get(code)
    if not entries:
        return None

    best: Optional[_AircraftInfo] = None
    for info in entries:
        if info.eff_from > as_of or as_of > info.eff_to:
            continue
        if best is None or info.eff_from > best.eff_from:
            best = info

    return best.name if best else None


# ============================================================
# Airport Code
# ============================================================

class AirportLookupResult(NamedTuple):
    city: str
    country: str
    region: str


class _AirportInfo(NamedTuple):
    city: str
    country: str
    region: str
    eff_from: date
    eff_to: date


_airport_cache: Optional[Dict[str, List[_AirportInfo]]] = None
_airport_cache_path: Optional[str] = None


def _load_airport_records(path: Optional[Path] = None) -> Dict[str, List[_AirportInfo]]:
    xlsx = path or _default_xlsx_path()
    if not xlsx.is_file():
        logger.warning(f"OAG airport reference missing: {xlsx}")
        return {}

    df = pd.read_excel(xlsx, sheet_name="Airport Code")
    need = {"IATA", "City Name", "Country Name", "Region Name", "Eff From", "Eff To"}
    missing = need - set(df.columns)
    if missing:
        logger.error(f"OAG airport sheet missing columns {missing}: {xlsx}")
        return {}

    by_code: Dict[str, List[_AirportInfo]] = {}
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
        by_code.setdefault(iata, []).append(_AirportInfo(city, country, region, eff_from, eff_to))

    logger.info(
        f"OAG airport reference loaded: {sum(len(v) for v in by_code.values())} rows, "
        f"{len(by_code)} unique airports from {xlsx.name}"
    )
    return by_code


def _get_airport_records(path: Optional[Path] = None) -> Dict[str, List[_AirportInfo]]:
    global _airport_cache, _airport_cache_path
    resolved = path or _default_xlsx_path()
    key = str(resolved.resolve())
    if _airport_cache is None or _airport_cache_path != key:
        _airport_cache = _load_airport_records(resolved)
        _airport_cache_path = key
    return _airport_cache


def lookup_airport(
    iata: Optional[str],
    as_of: date,
    *,
    path: Optional[Path] = None,
) -> Optional[AirportLookupResult]:
    """IATA 3글자 공항 코드와 기준일 as_of에 유효한 도시/국가/지역을 반환."""
    if not iata:
        return None
    code = str(iata).strip().upper()
    if not code or len(code) != 3:
        return None

    entries = _get_airport_records(path).get(code)
    if not entries:
        return None

    best: Optional[_AirportInfo] = None
    for info in entries:
        if info.eff_from > as_of or as_of > info.eff_to:
            continue
        if best is None or info.eff_from > best.eff_from:
            best = info

    if best is None:
        return None
    return AirportLookupResult(best.city, best.country, best.region)
