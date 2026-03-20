# Snowflake 쿼리 - OAG AIR_DREAMER_SCHED + MASTER_CARRIER / MASTER_LOCATION JOIN
# PostgreSQL queries.py와 동일한 출력 컬럼 별칭을 사용하여
# 다운스트림 코드 변경 없이 데이터소스 교체 가능
#
# 필터링 로직 (OAG 권장):
#   1. OPERATING <> 'N' → 운항편만 (코드쉐어 포함, 마케팅 전용편 제외)
#   2. SERVICE = 'J' → 여객편만 (화물편 제외)
#   3. FILE_DATE = FLIGHT_DATE - 1일 → 최신 파일로 중복 제거 + 처리 속도 향상
#   4. SELECT DISTINCT → 나머지 컬럼 중복 제거
#
# JOIN 구조 (OAG Caryen Cheong 제공 샘플 기반):
#   - MASTER_CARRIER_TRIAL: 항공사 코드 → 항공사명 매핑
#   - MASTER_LOCATION_TRIAL: 공항 코드 → 공항명/도시명/국가명 매핑
#   - COALESCE로 매핑 실패 시 원본 코드 유지 (Trial 뷰는 데이터 제한적)
#   - OAG_LOCATION_TYPE = 'AIRPORT' 조건으로 공항 레코드만 JOIN
#
# Snowflake는 별칭을 대문자로 반환하므로, 소문자 유지를 위해
# 모든 별칭을 쌍따옴표(")로 감싸야 함
#
# Named parameters: %(flight_date)s, %(airport)s → 호출 시 dict로 전달
# CTE로 출발편(DEPAPT) + 도착편(ARRAPT)을 UNION ALL 조회 후, JOIN은 1회만 수행

SELECT_AIRPORT_FLIGHTS_BOTH = """
WITH schedule_data AS (
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
      AND FILE_DATE = DATEADD(day, -1, %(flight_date)s::DATE)

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
      AND FILE_DATE = DATEADD(day, -1, %(flight_date)s::DATE)
)
SELECT
    s.FLIGHT_DATE                              AS "flight_date",
    s.FLTNO                                    AS "flight_number",
    s.CARRIER                                  AS "marketing_carrier_iata",
    s.CARRIER_CD_ICAO                          AS "marketing_carrier_icao",
    s.CARRIER                                  AS "operating_carrier_iata",
    s.CARRIER_CD_ICAO                          AS "operating_carrier_icao",
    COALESCE(c.CARRIER_NAME, s.SAD_NAME)       AS "operating_carrier_name",
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
    COALESCE(dep.OAG_CITY_STATE[0].CITY_NAME::STRING, s.DEPCITY)   AS "departure_city",
    COALESCE(dep.OAG_COUNTRY_NAME, s.DEPCTRY)                      AS "departure_country",
    COALESCE(dep.OAG_COUNTRY_CODE, s.DEPCTRY)                      AS "departure_country_code",
    NULL                                       AS "departure_region",
    dep.TIME_ZONE_CODE                         AS "departure_timezone",
    COALESCE(arr.OAG_CITY_STATE[0].CITY_NAME::STRING, s.ARRCITY)   AS "arrival_city",
    COALESCE(arr.OAG_COUNTRY_NAME, s.ARRCTRY)                      AS "arrival_country",
    COALESCE(arr.OAG_COUNTRY_CODE, s.ARRCTRY)                      AS "arrival_country_code",
    NULL                                       AS "arrival_region",
    arr.TIME_ZONE_CODE                         AS "arrival_timezone",
    s.FIRST_CLASS_SEATS                        AS "first_class_seat_count",
    s.BUSINESS_CLASS_SEATS                     AS "business_class_seat_count",
    s.PREMIUM_ECONOMY_CLASS_SEATS              AS "premium_economy_class_seat_count",
    s.ECONOMY_CLASS_SEATS                      AS "economy_class_seat_count",
    s.TOTAL_SEATS                              AS "total_seats"
FROM schedule_data s
LEFT JOIN OAG_SCHEDULES.DIRECT_CUSTOMER_CONFIGURATIONS.MASTER_CARRIER_TRIAL c
    ON s.CARRIER = c.IATA_CARRIER_CODE
LEFT JOIN OAG_SCHEDULES.DIRECT_CUSTOMER_CONFIGURATIONS.MASTER_LOCATION_TRIAL dep
    ON s.DEPAPT = dep.IATA_AIRPORT_CODE AND dep.OAG_LOCATION_TYPE = 'AIRPORT'
LEFT JOIN OAG_SCHEDULES.DIRECT_CUSTOMER_CONFIGURATIONS.MASTER_LOCATION_TRIAL arr
    ON s.ARRAPT = arr.IATA_AIRPORT_CODE AND arr.OAG_LOCATION_TYPE = 'AIRPORT'
ORDER BY "scheduled_departure_local"
"""
