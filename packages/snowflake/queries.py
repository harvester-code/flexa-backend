# Snowflake 쿼리 - OAG AIR_DREAMER_SCHED 뷰
# PostgreSQL queries.py와 동일한 출력 컬럼 별칭을 사용하여
# 다운스트림 코드 변경 없이 데이터소스 교체 가능
#
# 필터링 로직:
#   1. OPERATING = 'O' → 실제 운항편만 (코드쉐어 마케팅편/독립편 제외)
#   2. SELECT DISTINCT → OAG_SCHEDULE_FINGERPRINT, FILE_DATE 제외한 나머지 컬럼 중복 제거
#
# Snowflake는 별칭을 대문자로 반환하므로, 소문자 유지를 위해
# 모든 별칭을 쌍따옴표(")로 감싸야 함
#
# Named parameters: %(flight_date)s, %(airport)s → 호출 시 dict로 전달
# 출발편(DEPAPT) + 도착편(ARRAPT)을 UNION ALL로 조회하되 파라미터는 2개만 사용

SELECT_AIRPORT_FLIGHTS_BOTH = """
SELECT DISTINCT
    FLIGHT_DATE as "flight_date",
    FLTNO as "flight_number",
    CARRIER as "marketing_carrier_iata",
    CARRIER_CD_ICAO as "marketing_carrier_icao",
    CARRIER as "operating_carrier_iata",
    CARRIER_CD_ICAO as "operating_carrier_icao",
    SAD_NAME as "operating_carrier_name",
    DEPAPT as "departure_airport_iata",
    DEP_PORT_CD_ICAO as "departure_airport_icao",
    ARRAPT as "arrival_airport_iata",
    ARR_PORT_CD_ICAO as "arrival_airport_icao",
    SCHEDULED_DEPARTURE_DATE_TIME_LOCAL as "scheduled_departure_local",
    SCHEDULED_DEPARTURE_DATE_TIME_UTC as "scheduled_departure_utc",
    SCHEDULED_ARRIVAL_DATE_TIME_LOCAL as "scheduled_arrival_local",
    SCHEDULED_ARRIVAL_DATE_TIME_UTC as "scheduled_arrival_utc",
    INPACFT as "aircraft_type_iata",
    EQUIPMENT_CD_ICAO as "aircraft_type_icao",
    DEPTERM as "departure_terminal",
    ARRTERM as "arrival_terminal",
    'snowflake' as "data_source",
    DOMINT as "flight_type",
    DEPCITY as "departure_city",
    DEPCTRY as "departure_country",
    NULL as "departure_country_code",
    NULL as "departure_region",
    NULL as "departure_timezone",
    ARRCITY as "arrival_city",
    ARRCTRY as "arrival_country",
    NULL as "arrival_country_code",
    NULL as "arrival_region",
    NULL as "arrival_timezone",
    FIRST_CLASS_SEATS as "first_class_seat_count",
    BUSINESS_CLASS_SEATS as "business_class_seat_count",
    PREMIUM_ECONOMY_CLASS_SEATS as "premium_economy_class_seat_count",
    ECONOMY_CLASS_SEATS as "economy_class_seat_count",
    TOTAL_SEATS as "total_seats"
FROM OAG_SCHEDULES.DIRECT_CUSTOMER_CONFIGURATIONS.AIR_DREAMER_SCHED
WHERE FLIGHT_DATE = %(flight_date)s
  AND DEPAPT = %(airport)s
  AND OPERATING = 'O'

UNION ALL

SELECT DISTINCT
    FLIGHT_DATE as "flight_date",
    FLTNO as "flight_number",
    CARRIER as "marketing_carrier_iata",
    CARRIER_CD_ICAO as "marketing_carrier_icao",
    CARRIER as "operating_carrier_iata",
    CARRIER_CD_ICAO as "operating_carrier_icao",
    SAD_NAME as "operating_carrier_name",
    DEPAPT as "departure_airport_iata",
    DEP_PORT_CD_ICAO as "departure_airport_icao",
    ARRAPT as "arrival_airport_iata",
    ARR_PORT_CD_ICAO as "arrival_airport_icao",
    SCHEDULED_DEPARTURE_DATE_TIME_LOCAL as "scheduled_departure_local",
    SCHEDULED_DEPARTURE_DATE_TIME_UTC as "scheduled_departure_utc",
    SCHEDULED_ARRIVAL_DATE_TIME_LOCAL as "scheduled_arrival_local",
    SCHEDULED_ARRIVAL_DATE_TIME_UTC as "scheduled_arrival_utc",
    INPACFT as "aircraft_type_iata",
    EQUIPMENT_CD_ICAO as "aircraft_type_icao",
    DEPTERM as "departure_terminal",
    ARRTERM as "arrival_terminal",
    'snowflake' as "data_source",
    DOMINT as "flight_type",
    DEPCITY as "departure_city",
    DEPCTRY as "departure_country",
    NULL as "departure_country_code",
    NULL as "departure_region",
    NULL as "departure_timezone",
    ARRCITY as "arrival_city",
    ARRCTRY as "arrival_country",
    NULL as "arrival_country_code",
    NULL as "arrival_region",
    NULL as "arrival_timezone",
    FIRST_CLASS_SEATS as "first_class_seat_count",
    BUSINESS_CLASS_SEATS as "business_class_seat_count",
    PREMIUM_ECONOMY_CLASS_SEATS as "premium_economy_class_seat_count",
    ECONOMY_CLASS_SEATS as "economy_class_seat_count",
    TOTAL_SEATS as "total_seats"
FROM OAG_SCHEDULES.DIRECT_CUSTOMER_CONFIGURATIONS.AIR_DREAMER_SCHED
WHERE FLIGHT_DATE = %(flight_date)s
  AND ARRAPT = %(airport)s
  AND OPERATING = 'O'

ORDER BY "scheduled_departure_local"
"""
