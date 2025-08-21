# 항공편 데이터 조회 쿼리 (통일된 35개 컬럼 구조)
# - 기본 항공편 정보 20개 + 공항 정보 10개 + 좌석 정보 5개
# - 날짜에 따라 적절한 테이블 선택 (과거: flights_extended, 미래: schedule)

# 과거 데이터용 쿼리 (flights_extended 테이블)
SELECT_AIRPORT_FLIGHTS_EXTENDED = """
SELECT 
    -- 기본 항공편 정보
    fe.flight_date_local as flight_date,
    fe.flight_number,
    fe.marketing_carrier_iata,
    fe.marketing_carrier_icao,
    fe.operating_carrier_iata,
    fe.operating_carrier_icao,
    fe.operating_carrier_name,
    fe.departure_airport_iata,
    fe.departure_airport_icao,
    fe.arrival_airport_iata,
    fe.arrival_airport_icao,
    fe.published_departure_local as scheduled_departure_local,
    fe.published_departure as scheduled_departure_utc,
    fe.published_arrival_local as scheduled_arrival_local,
    fe.published_arrival as scheduled_arrival_utc,
    fe.aircraft_code_iata as aircraft_type_iata,
    fe.aircraft_code_icao as aircraft_type_icao,
    fe.departure_terminal,
    fe.arrival_terminal,
    'flights_extended' as data_source,
    
    -- flight_type 계산
    CASE
        WHEN dep_apt.country_code = arr_apt.country_code THEN 'Domestic'
        ELSE 'International'
    END AS flight_type,
    
    -- 출발 공항 정보
    dep_apt.city as departure_city,
    dep_apt.country_name as departure_country,
    dep_apt.country_code as departure_country_code,
    dep_apt.region_name as departure_region,
    dep_apt.timezone_region_name as departure_timezone,
    
    -- 도착 공항 정보
    arr_apt.city as arrival_city,
    arr_apt.country_name as arrival_country,
    arr_apt.country_code as arrival_country_code,
    arr_apt.region_name as arrival_region,
    arr_apt.timezone_region_name as arrival_timezone,
    
    -- 좌석수 정보
    fe.first_class_seat_count,
    fe.business_class_seat_count,
    fe.premium_economy_class_seat_count,
    fe.economy_class_seat_count,
    (COALESCE(fe.first_class_seat_count,0) + 
     COALESCE(fe.business_class_seat_count,0) + 
     COALESCE(fe.premium_economy_class_seat_count,0) + 
     COALESCE(fe.economy_class_seat_count,0)) as total_seats
     
FROM flights_extended fe
LEFT JOIN airports dep_apt ON fe.departure_airport_iata = dep_apt.airport_iata
LEFT JOIN airports arr_apt ON fe.arrival_airport_iata = arr_apt.airport_iata
WHERE fe.flight_date_local = %s
AND (fe.departure_airport_iata = %s OR fe.arrival_airport_iata = %s)
AND (COALESCE(fe.first_class_seat_count,0) + 
     COALESCE(fe.business_class_seat_count,0) + 
     COALESCE(fe.premium_economy_class_seat_count,0) + 
     COALESCE(fe.economy_class_seat_count,0)) > 0
ORDER BY fe.published_departure_local
"""

# 오늘/미래 데이터용 쿼리 (schedule 테이블)
SELECT_AIRPORT_SCHEDULE = """
SELECT 
    -- 기본 항공편 정보
    s.operating_date_local as flight_date,
    s.flight_number,
    s.marketing_carrier_iata,
    s.marketing_carrier_icao,
    s.operating_carrier_iata,
    s.operating_carrier_icao,
    c.name as operating_carrier_name,
    s.departure_station_code_iata as departure_airport_iata,
    s.departure_station_code_icao as departure_airport_icao,
    s.arrival_station_code_iata as arrival_airport_iata,
    s.arrival_station_code_icao as arrival_airport_icao,
    s.passenger_departure_time_local as scheduled_departure_local,
    s.passenger_departure_time_utc as scheduled_departure_utc,
    s.passenger_arrival_time_local as scheduled_arrival_local,
    s.passenger_arrival_time_utc as scheduled_arrival_utc,
    s.equipment_subtype_code_iata as aircraft_type_iata,
    s.equipment_subtype_code_icao as aircraft_type_icao,
    s.departure_terminal,
    s.arrival_terminal,
    'schedule' as data_source,
    
    -- flight_type 계산
    CASE
        WHEN dep_apt.country_code = arr_apt.country_code THEN 'Domestic'
           ELSE 'International'
    END AS flight_type,
    
    -- 출발 공항 정보
    dep_apt.city as departure_city,
    dep_apt.country_name as departure_country,
    dep_apt.country_code as departure_country_code,
    dep_apt.region_name as departure_region,
    dep_apt.timezone_region_name as departure_timezone,
    
    -- 도착 공항 정보
    arr_apt.city as arrival_city,
    arr_apt.country_name as arrival_country,
    arr_apt.country_code as arrival_country_code,
    arr_apt.region_name as arrival_region,
    arr_apt.timezone_region_name as arrival_timezone,
    
    -- 좌석수 정보
    s.first_class_seats as first_class_seat_count,
    s.business_class_seats as business_class_seat_count,
    s.premium_economy_seats as premium_economy_class_seat_count,
    s.economy_class_seats as economy_class_seat_count,
    s.total_seats
    
FROM schedule s
LEFT JOIN airports dep_apt ON s.departure_station_code_iata = dep_apt.airport_iata
LEFT JOIN airports arr_apt ON s.arrival_station_code_iata = arr_apt.airport_iata
LEFT JOIN carriers c ON c.carrier_iata = s.operating_carrier_iata
WHERE s.operating_date_local = %s
AND (s.departure_station_code_iata = %s OR s.arrival_station_code_iata = %s)
AND s.total_seats > 0
AND s.is_codeshare = 0
ORDER BY s.passenger_departure_time_local
"""
