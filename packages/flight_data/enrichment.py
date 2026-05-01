"""
항공편 데이터 보강 모듈 (Flight Data Enrichment)

oag_ref.xlsx 기반으로 raw 쿼리 결과를 후처리:
  1. 항공사 IATA 코드 → 항공사명 (Airline Code 시트, 비행일 기준 유효 행)
  2. 항공기 IATA 코드 → 기종명  (Aircraft Code 시트)
  3. 공항 IATA 코드 → 도시/국가/지역 (Airport Code 시트)
"""

from datetime import date
from typing import List
from loguru import logger

from .oag_reference import lookup_airline_name, parse_flight_date, lookup_aircraft_name, lookup_airport
from .flight_number import normalize_flight_number


def enrich_flight_data(flights: List[dict]) -> List[dict]:
    """
    항공편 데이터 리스트를 oag_ref.xlsx 기준으로 보강.

    처리 대상:
      - operating_carrier_name  : 항공사 IATA → 항공사명
      - flight_number           : carrier prefix 없을 때 정규화
      - aircraft_type_name      : 항공기 IATA → 기종명
      - departure_city/country/region : 출발 공항 IATA → 상세정보
      - arrival_city/country/region   : 도착 공항 IATA → 상세정보
    """
    enriched_count = 0

    for flight in flights:
        changed = False
        carrier_code = flight.get("marketing_carrier_iata") or flight.get("operating_carrier_iata")
        as_of = parse_flight_date(flight.get("flight_date")) or date.today()

        # 항공사명
        oag_name = lookup_airline_name(carrier_code, as_of)
        if oag_name and oag_name != flight.get("operating_carrier_name"):
            flight["operating_carrier_name"] = oag_name
            changed = True

        # 편명 정규화 (carrier prefix 없는 경우만)
        raw_fn = flight.get("flight_number")
        if raw_fn is not None and carrier_code:
            fn_str = str(raw_fn).strip()
            if fn_str and not fn_str.upper().startswith(carrier_code.upper()):
                normalized = normalize_flight_number(carrier_code, fn_str)
                if normalized and normalized != fn_str:
                    flight["flight_number"] = normalized
                    changed = True

        # 항공기종명
        acft_iata = flight.get("aircraft_type_iata")
        if acft_iata:
            acft_name = lookup_aircraft_name(acft_iata, as_of)
            if acft_name:
                flight["aircraft_type_name"] = acft_name
                changed = True

        # 출발 공항 정보
        dep_oag = lookup_airport(flight.get("departure_airport_iata"), as_of)
        if dep_oag:
            flight["departure_city"] = dep_oag.city
            flight["departure_country"] = dep_oag.country
            flight["departure_region"] = dep_oag.region
            changed = True

        # 도착 공항 정보
        arr_oag = lookup_airport(flight.get("arrival_airport_iata"), as_of)
        if arr_oag:
            flight["arrival_city"] = arr_oag.city
            flight["arrival_country"] = arr_oag.country
            flight["arrival_region"] = arr_oag.region
            changed = True

        if changed:
            enriched_count += 1

    if enriched_count > 0:
        logger.info(f"Flight data enrichment: {enriched_count}/{len(flights)} flights updated")

    return flights
