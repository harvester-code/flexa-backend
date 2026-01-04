# PostgreSQL 쿼리 - 인덱스 최적화 구조
# - 날짜 조건 우선 (인덱스 첫 번째 컬럼 활용)
# - UNION으로 출발/도착 분리하여 각 복합 인덱스 활용
#   * 출발편: idx_date_dep 인덱스 활용 (Time series, Dep Airport Code)
#   * 도착편: idx_date_arr 인덱스 활용 (Time series, Arr Airport Code)
# - 날짜 범위 조건으로 idx_time_series 인덱스도 활용 가능

# ✅ 출발편 + 도착편 한 번에 조회 (UNION으로 인덱스 최적화)
SELECT_AIRPORT_FLIGHTS_BOTH = """
SELECT 
    -- 기본 항공편 정보
    "Time series"::date as flight_date,
    NULL as flight_number,
    "Carrier Code" as marketing_carrier_iata,
    NULL as marketing_carrier_icao,
    "Carrier Code" as operating_carrier_iata,
    NULL as operating_carrier_icao,
    "Carrier Name" as operating_carrier_name,
    "Dep Airport Code" as departure_airport_iata,
    NULL as departure_airport_icao,
    "Arr Airport Code" as arrival_airport_iata,
    NULL as arrival_airport_icao,
    "Local Time_x" as scheduled_departure_local,
    "UTC Time_x" as scheduled_departure_utc,
    "Local Time_y" as scheduled_arrival_local,
    "UTC Time_y" as scheduled_arrival_utc,
    "Specific Aircraft Code" as aircraft_type_iata,
    NULL as aircraft_type_icao,
    "Dep Terminal" as departure_terminal,
    "Arr Terminal" as arrival_terminal,
    'postgresql' as data_source,
    
    "International/Domestic" as flight_type,
    
    "City Name_x" as departure_city,
    "Country Name_x" as departure_country,
    "Dep IATA Country Code" as departure_country_code,
    "Region Name_x" as departure_region,
    NULL as departure_timezone,
    
    "City Name_y" as arrival_city,
    "Country Name_y" as arrival_country,
    "Arr IATA Country Code" as arrival_country_code,
    "Region Name_y" as arrival_region,
    NULL as arrival_timezone,
    
    NULL::integer as first_class_seat_count,
    NULL::integer as business_class_seat_count,
    NULL::integer as premium_economy_class_seat_count,
    NULL::integer as economy_class_seat_count,
    "Seats (Total)"::integer as total_seats
     
FROM utc_world_all_years
WHERE "Time series" = %s
  AND "Dep Airport Code" = %s
  AND "Seats (Total)" > 0

UNION ALL

SELECT 
    -- 기본 항공편 정보
    "Time series"::date as flight_date,
    NULL as flight_number,
    "Carrier Code" as marketing_carrier_iata,
    NULL as marketing_carrier_icao,
    "Carrier Code" as operating_carrier_iata,
    NULL as operating_carrier_icao,
    "Carrier Name" as operating_carrier_name,
    "Dep Airport Code" as departure_airport_iata,
    NULL as departure_airport_icao,
    "Arr Airport Code" as arrival_airport_iata,
    NULL as arrival_airport_icao,
    "Local Time_x" as scheduled_departure_local,
    "UTC Time_x" as scheduled_departure_utc,
    "Local Time_y" as scheduled_arrival_local,
    "UTC Time_y" as scheduled_arrival_utc,
    "Specific Aircraft Code" as aircraft_type_iata,
    NULL as aircraft_type_icao,
    "Dep Terminal" as departure_terminal,
    "Arr Terminal" as arrival_terminal,
    'postgresql' as data_source,
    
    "International/Domestic" as flight_type,
    
    "City Name_x" as departure_city,
    "Country Name_x" as departure_country,
    "Dep IATA Country Code" as departure_country_code,
    "Region Name_x" as departure_region,
    NULL as departure_timezone,
    
    "City Name_y" as arrival_city,
    "Country Name_y" as arrival_country,
    "Arr IATA Country Code" as arrival_country_code,
    "Region Name_y" as arrival_region,
    NULL as arrival_timezone,
    
    NULL::integer as first_class_seat_count,
    NULL::integer as business_class_seat_count,
    NULL::integer as premium_economy_class_seat_count,
    NULL::integer as economy_class_seat_count,
    "Seats (Total)"::integer as total_seats
     
FROM utc_world_all_years
WHERE "Time series" = %s
  AND "Arr Airport Code" = %s
  AND "Seats (Total)" > 0

ORDER BY scheduled_departure_local
"""



