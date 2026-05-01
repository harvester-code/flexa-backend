"""
편명(Flight Number) 정규화 유틸리티

프로젝트 전체 공통 규칙:
  편명 = 항공사 IATA 코드(2-3자) + 최소 3자리 편번호
  예시: KE712, KE012, AZ076, PR0017 → KE712, KE012, AZ076, PR017

규칙:
  1. 편번호(flight_no) 앞의 0을 모두 제거
  2. 제거 후 3자리 미만이면 앞에 0을 채워 최소 3자리 보장
  3. carrier_code + 정규화된 편번호 결합

SQL 동등 표현 (PostgreSQL):
  "Carrier Code" || LPAD(LTRIM("Flight No", '0'), 3, '0')
"""

from typing import Optional


def normalize_flight_number(
    carrier_code: Optional[str],
    flight_no: Optional[str],
) -> Optional[str]:
    """
    항공사 코드와 편번호를 받아 KE012 형식의 정규화된 편명을 반환합니다.

    Args:
        carrier_code: 항공사 IATA 코드 (예: "KE", "PR", "AZ")
        flight_no:    편번호 문자열 (예: "712", "0712", "0017", "12")

    Returns:
        정규화된 편명 (예: "KE712", "KE712", "AZ017", "KE012")
        유효하지 않은 입력이면 None 반환
    """
    if not carrier_code or flight_no is None:
        return None

    carrier = str(carrier_code).strip()
    num_str = str(flight_no).strip()

    if not carrier or not num_str:
        return None

    # 앞의 0 모두 제거 후 최소 3자리 보장
    stripped = num_str.lstrip("0")
    if not stripped:
        stripped = "0"
    normalized_num = stripped.zfill(3)

    return carrier + normalized_num


def build_flight_id(flight: dict) -> Optional[str]:
    """
    항공편 dict에서 KE012 형식의 고유 편명 ID를 생성합니다.

    flight_number 컬럼이 이미 정규화되어 있으면 그대로 사용하고,
    없으면 operating_carrier_iata + flight_number 원본으로 생성합니다.
    """
    fn = flight.get("flight_number")
    if fn:
        fn_str = str(fn).strip()
        if fn_str:
            return fn_str

    # flight_number 컬럼이 없거나 비어있으면 carrier + raw 번호로 생성
    carrier = flight.get("operating_carrier_iata") or flight.get("marketing_carrier_iata")
    return normalize_flight_number(carrier, fn)


def build_flight_id_from_row(row) -> Optional[str]:
    """
    pandas Series row에서 KE012 형식의 고유 편명 ID를 생성합니다.
    """
    import pandas as pd

    fn = row.get("flight_number") if hasattr(row, "get") else None
    if fn is not None and pd.notna(fn) and str(fn).strip():
        return str(fn).strip()

    carrier_raw = row.get("operating_carrier_iata") if hasattr(row, "get") else None
    carrier = str(carrier_raw).strip() if carrier_raw is not None and pd.notna(carrier_raw) else None
    return normalize_flight_number(carrier, fn)
