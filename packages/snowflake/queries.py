# Snowflake 쿼리 - OAG AIR_DREAMER_SCHED (항공사/공항/항공기 정보는 oag_ref.xlsx에서 후처리)
# PostgreSQL queries.py와 동일한 출력 컬럼 별칭을 사용하여
# 다운스트림 코드 변경 없이 데이터소스 교체 가능
#
# 필터링 로직 (OAG 권장):
#   1. OPERATING <> 'N' → 운항편만 (코드쉐어 포함, 마케팅 전용편 제외)
#   2. SERVICE = 'J' → 여객편만 (화물편 SERVICE='F' 등 제외)
#   3. TOTAL_SEATS > 0 → 좌석 데이터 누락된 불완전 레코드 제외 (NULL/0 모두 제거)
#   4. FILE_DATE = MAX(FILE_DATE) → 해당 비행일의 가장 최신 스냅샷만 사용 (OAG 적재 시점 무관)
#   5. SELECT DISTINCT → 나머지 컬럼 중복 제거
#
# MASTER DATA JOIN 없음 — 항공사·공항 정보 모두 packages.flight_data.enrichment에서
# oag_ref.xlsx (Airline Code / Airport Code 시트) 기반으로 후처리
#
# Snowflake는 별칭을 대문자로 반환하므로, 소문자 유지를 위해
# 모든 별칭을 쌍따옴표(")로 감싸야 함
#
# Named parameters: %(flight_date)s, %(airport)s → 호출 시 dict로 전달
# CTE로 출발편(DEPAPT) + 도착편(ARRAPT)을 UNION ALL 조회 후, JOIN은 1회만 수행

SELECT_AIRPORT_FLIGHTS_BOTH = """
WITH latest_file AS (
    SELECT MAX(FILE_DATE) AS max_file_date
    FROM OAG_SCHEDULES.DIRECT_CUSTOMER_CONFIGURATIONS.AIR_DREAMER_SCHED
    WHERE FLIGHT_DATE = %(flight_date)s
),
schedule_data AS (
    SELECT DISTINCT
        FLIGHT_DATE,
        FLTNO,
        CARRIER,
        CARRIER_CD_ICAO,
        SAD_NAME,
        DEPAPT,
        DEP_PORT_CD_ICAO,
        ARRAPT,
        ARR_PORT_CD_ICAO,
        SCHEDULED_DEPARTURE_DATE_TIME_LOCAL,
        SCHEDULED_DEPARTURE_DATE_TIME_UTC,
        SCHEDULED_ARRIVAL_DATE_TIME_LOCAL,
        SCHEDULED_ARRIVAL_DATE_TIME_UTC,
        INPACFT,
        EQUIPMENT_CD_ICAO,
        DEPTERM,
        ARRTERM,
        DEPCTRY,
        ARRCTRY,
        DEPCITY,
        ARRCITY,
        FIRST_CLASS_SEATS,
        BUSINESS_CLASS_SEATS,
        PREMIUM_ECONOMY_CLASS_SEATS,
        ECONOMY_CLASS_SEATS,
        TOTAL_SEATS
    FROM OAG_SCHEDULES.DIRECT_CUSTOMER_CONFIGURATIONS.AIR_DREAMER_SCHED
    WHERE FLIGHT_DATE = %(flight_date)s
      AND DEPAPT = %(airport)s
      AND OPERATING <> 'N'
      AND SERVICE = 'J'
      AND TOTAL_SEATS > 0
      AND FILE_DATE = (SELECT max_file_date FROM latest_file)

    UNION ALL

    SELECT DISTINCT
        FLIGHT_DATE,
        FLTNO,
        CARRIER,
        CARRIER_CD_ICAO,
        SAD_NAME,
        DEPAPT,
        DEP_PORT_CD_ICAO,
        ARRAPT,
        ARR_PORT_CD_ICAO,
        SCHEDULED_DEPARTURE_DATE_TIME_LOCAL,
        SCHEDULED_DEPARTURE_DATE_TIME_UTC,
        SCHEDULED_ARRIVAL_DATE_TIME_LOCAL,
        SCHEDULED_ARRIVAL_DATE_TIME_UTC,
        INPACFT,
        EQUIPMENT_CD_ICAO,
        DEPTERM,
        ARRTERM,
        DEPCTRY,
        ARRCTRY,
        DEPCITY,
        ARRCITY,
        FIRST_CLASS_SEATS,
        BUSINESS_CLASS_SEATS,
        PREMIUM_ECONOMY_CLASS_SEATS,
        ECONOMY_CLASS_SEATS,
        TOTAL_SEATS
    FROM OAG_SCHEDULES.DIRECT_CUSTOMER_CONFIGURATIONS.AIR_DREAMER_SCHED
    WHERE FLIGHT_DATE = %(flight_date)s
      AND ARRAPT = %(airport)s
      AND OPERATING <> 'N'
      AND SERVICE = 'J'
      AND TOTAL_SEATS > 0
      AND FILE_DATE = (SELECT max_file_date FROM latest_file)
)
SELECT
    s.FLIGHT_DATE                              AS "flight_date",
    s.FLTNO                                    AS "flight_number",
    s.CARRIER                                  AS "marketing_carrier_iata",
    s.CARRIER_CD_ICAO                          AS "marketing_carrier_icao",
    s.CARRIER                                  AS "operating_carrier_iata",
    s.CARRIER_CD_ICAO                          AS "operating_carrier_icao",
    s.SAD_NAME                                 AS "operating_carrier_name",
    s.DEPAPT                                   AS "departure_airport_iata",
    s.DEP_PORT_CD_ICAO                         AS "departure_airport_icao",
    s.ARRAPT                                   AS "arrival_airport_iata",
    s.ARR_PORT_CD_ICAO                         AS "arrival_airport_icao",
    s.SCHEDULED_DEPARTURE_DATE_TIME_LOCAL       AS "scheduled_departure_local",
    s.SCHEDULED_DEPARTURE_DATE_TIME_UTC         AS "scheduled_departure_utc",
    s.SCHEDULED_ARRIVAL_DATE_TIME_LOCAL         AS "scheduled_arrival_local",
    s.SCHEDULED_ARRIVAL_DATE_TIME_UTC           AS "scheduled_arrival_utc",
    s.INPACFT                                  AS "aircraft_type_iata",
    s.EQUIPMENT_CD_ICAO                        AS "aircraft_type_icao",
    s.DEPTERM                                  AS "departure_terminal",
    s.ARRTERM                                  AS "arrival_terminal",
    'snowflake'                                AS "data_source",
    CASE WHEN s.DEPCTRY = s.ARRCTRY THEN 'Domestic' ELSE 'International' END AS "flight_type",
    s.DEPCITY                                  AS "departure_city",
    s.DEPCTRY                                  AS "departure_country",
    s.DEPCTRY                                  AS "departure_country_code",
    NULL                                       AS "departure_region",
    NULL                                       AS "departure_timezone",
    s.ARRCITY                                  AS "arrival_city",
    s.ARRCTRY                                  AS "arrival_country",
    s.ARRCTRY                                  AS "arrival_country_code",
    NULL                                       AS "arrival_region",
    NULL                                       AS "arrival_timezone",
    s.FIRST_CLASS_SEATS                        AS "first_class_seat_count",
    s.BUSINESS_CLASS_SEATS                     AS "business_class_seat_count",
    s.PREMIUM_ECONOMY_CLASS_SEATS              AS "premium_economy_class_seat_count",
    s.ECONOMY_CLASS_SEATS                      AS "economy_class_seat_count",
    s.TOTAL_SEATS                              AS "total_seats"
FROM schedule_data s
ORDER BY "scheduled_departure_local"
"""
