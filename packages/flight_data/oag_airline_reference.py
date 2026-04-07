"""
OAG 항공사 코드 레퍼런스 (packages/oag_ref.xlsx — Airline Code 시트)

Snowflake MASTER_CARRIER 대신 사용. Eff From / Eff To로 유효 기간이 나뉘며,
OAG 엑셀에서 연도가 한 세기 밀려 들어오는 경우(2038→1938 등)를
2자리 윈도우 방식으로 보정: 끝 2자리 < 70 → 20xx, >= 70 → 19xx.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
from loguru import logger

AIRLINE_CODE_SHEET = "Airline Code"

Record = Tuple[str, str, date, date]


def _default_xlsx_path() -> Path:
    return Path(__file__).resolve().parent.parent / "oag_ref.xlsx"


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


def _load_records(path: Optional[Path] = None) -> List[Record]:
    xlsx = path or _default_xlsx_path()
    if not xlsx.is_file():
        logger.warning(f"OAG airline reference missing: {xlsx}")
        return []

    df = pd.read_excel(xlsx, sheet_name=AIRLINE_CODE_SHEET)
    need = {"IATA", "Airline Name", "Eff From", "Eff To"}
    missing = need - set(df.columns)
    if missing:
        logger.error(f"OAG airline sheet missing columns {missing}: {xlsx}")
        return []

    rows: List[Record] = []
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


_records_cache: Optional[List[Record]] = None
_cache_path: Optional[str] = None


def _get_records(path: Optional[Path] = None) -> List[Record]:
    global _records_cache, _cache_path
    resolved = path or _default_xlsx_path()
    key = str(resolved.resolve())
    if _records_cache is None or _cache_path != key:
        _records_cache = _load_records(resolved)
        _cache_path = key
    return _records_cache


def lookup_airline_name(
    iata: Optional[str],
    as_of: date,
    *,
    path: Optional[Path] = None,
) -> Optional[str]:
    """
    IATA 코드와 기준일 as_of에 유효한 항공사 표시명을 반환.
    여러 행이 겹치면 Eff From이 가장 늦은 행을 사용.
    """
    if not iata:
        return None
    code = str(iata).strip().upper()
    if not code:
        return None

    best: Optional[Tuple[date, str]] = None
    for row_code, name, eff_from, eff_to in _get_records(path):
        if row_code != code:
            continue
        if eff_from > as_of:
            continue
        if as_of > eff_to:
            continue
        cand: Tuple[date, str] = (eff_from, name)
        if best is None or cand[0] > best[0]:
            best = cand

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
