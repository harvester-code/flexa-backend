# PostgreSQL 쿼리 - 인덱스 최적화 구조
# - 날짜 조건 우선 (인덱스 첫 번째 컬럼 활용)
# - UNION으로 출발/도착 분리하여 각 복합 인덱스 활용
#   * 출발편: idx_date_dep 인덱스 활용 (Time series, Dep Airport Code)
#   * 도착편: idx_date_arr 인덱스 활용 (Time series, Arr Airport Code)
# - 날짜 범위 조건으로 idx_time_series 인덱스도 활용 가능
#
# Named parameters: %(flight_date)s, %(airport)s → 호출 시 dict로 전달
# 출발편 + 도착편을 UNION ALL로 조회하되 파라미터는 2개만 사용

SELECT_AIRPORT_FLIGHTS_BOTH = """
SELECT 
    "Time series"::date as flight_date,
    "Carrier Code" || CASE
        WHEN LENGTH(LTRIM("Flight No", '0')) >= 3 THEN LTRIM("Flight No", '0')
        ELSE LPAD(LTRIM("Flight No", '0'), 3, '0')
    END as flight_number,
    "Carrier Code" as marketing_carrier_iata,
    "Carrier Code" as operating_carrier_iata,
    "Carrier Name" as operating_carrier_name,
    "Dep Airport Code" as departure_airport_iata,
    "Arr Airport Code" as arrival_airport_iata,
    "Local Time_x" as scheduled_departure_local,
    "UTC Time_x" as scheduled_departure_utc,
    "Local Time_y" as scheduled_arrival_local,
    "UTC Time_y" as scheduled_arrival_utc,
    "Specific Aircraft Code" as aircraft_type_iata,
    "Dep Terminal" as departure_terminal,
    "Arr Terminal" as arrival_terminal,
    'postgresql' as data_source,
    "International/Domestic" as flight_type,
    "City Name_x" as departure_city,
    "Country Name_x" as departure_country,
    "Region Name_x" as departure_region,
    "City Name_y" as arrival_city,
    "Country Name_y" as arrival_country,
    "Region Name_y" as arrival_region,
    "First Seats"::integer as first_class_seat_count,
    "Business Seats"::integer as business_class_seat_count,
    NULL::integer as premium_economy_class_seat_count,
    "Economy Seats"::integer as economy_class_seat_count,
    "Seats"::integer as total_seats
     
FROM "oag-schedule"
WHERE "Time series" = %(flight_date)s
  AND "Dep Airport Code" = %(airport)s
  AND "Seats"::integer > 0

UNION ALL

SELECT 
    "Time series"::date as flight_date,
    "Carrier Code" || CASE
        WHEN LENGTH(LTRIM("Flight No", '0')) >= 3 THEN LTRIM("Flight No", '0')
        ELSE LPAD(LTRIM("Flight No", '0'), 3, '0')
    END as flight_number,
    "Carrier Code" as marketing_carrier_iata,
    "Carrier Code" as operating_carrier_iata,
    "Carrier Name" as operating_carrier_name,
    "Dep Airport Code" as departure_airport_iata,
    "Arr Airport Code" as arrival_airport_iata,
    "Local Time_x" as scheduled_departure_local,
    "UTC Time_x" as scheduled_departure_utc,
    "Local Time_y" as scheduled_arrival_local,
    "UTC Time_y" as scheduled_arrival_utc,
    "Specific Aircraft Code" as aircraft_type_iata,
    "Dep Terminal" as departure_terminal,
    "Arr Terminal" as arrival_terminal,
    'postgresql' as data_source,
    "International/Domestic" as flight_type,
    "City Name_x" as departure_city,
    "Country Name_x" as departure_country,
    "Region Name_x" as departure_region,
    "City Name_y" as arrival_city,
    "Country Name_y" as arrival_country,
    "Region Name_y" as arrival_region,
    "First Seats"::integer as first_class_seat_count,
    "Business Seats"::integer as business_class_seat_count,
    NULL::integer as premium_economy_class_seat_count,
    "Economy Seats"::integer as economy_class_seat_count,
    "Seats"::integer as total_seats
     
FROM "oag-schedule"
WHERE "Time series" = %(flight_date)s
  AND "Arr Airport Code" = %(airport)s
  AND "Seats"::integer > 0

ORDER BY scheduled_departure_local
"""
