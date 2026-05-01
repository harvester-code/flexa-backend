"""
Flight Filters Metadata Processing Module

This module handles flight filtering metadata generation:
- FlightFiltersResponse: Generates filter options JSON for Departure/Arrival modes (based on real data)
"""

from typing import Dict, Any, List
from collections import defaultdict
from datetime import datetime
from loguru import logger
from sqlalchemy import Connection

# ========================================
# DATABASE QUERY IMPORTS
# ========================================
# 🟢 Provider Pattern: FLIGHT_DATA_SOURCE 환경변수로 PostgreSQL/Snowflake 자동 전환
from packages.flight_data import SELECT_AIRPORT_FLIGHTS_BOTH, enrich_flight_data

# 🔴 Redshift (Legacy - Commented out for reference)
# from ..queries import SELECT_AIRPORT_FLIGHTS_EXTENDED, SELECT_AIRPORT_SCHEDULE


class FlightFiltersResponse:
    """Flight filters metadata response generation class (based on real data)"""

    async def generate_filters_metadata(
        self, 
        snowflake_db: Connection,  # 🔵 Snowflake (Current)
        # redshift_db: Connection,  # 🔴 Redshift (Legacy - for reference)
        scenario_id: str, 
        airport: str, 
        date: str
    ) -> Dict[str, Any]:
        """
        Generate flight filters metadata based on real data

        Args:
            snowflake_db: Snowflake database connection (Current)
            # redshift_db: Redshift database connection (Legacy)
            scenario_id: Scenario ID
            airport: Airport IATA code (e.g. ICN, KPO) - case insensitive
            date: Target date (YYYY-MM-DD)

        Returns:
            Dict: Filter metadata for departure/arrival modes
        """
        # Normalize airport code to uppercase for consistency
        airport = airport.upper().strip()

        logger.info(f"🔍 Generating flight filters metadata for scenario {scenario_id}")
        logger.info(f"📍 Parameters: airport={airport}, date={date}")

        # ========================================
        # 🔵 Snowflake: 단일 쿼리로 모든 데이터 조회 (현재 활성)
        # ========================================
        try:
            logger.info("✈️  Fetching ALL flight data (departure + arrival) in one query...")
            all_flights = await self._fetch_both_flights(
                snowflake_db, airport, date, scenario_id
            )
            logger.info(f"✅ All flight data fetched: {len(all_flights)} flights")

            # 2. Separate departure and arrival flights in Python
            departure_data = [
                flight for flight in all_flights 
                if flight.get("departure_airport_iata") == airport
            ]
            arrival_data = [
                flight for flight in all_flights 
                if flight.get("arrival_airport_iata") == airport
            ]
            
            logger.info(f"📊 Separated: {len(departure_data)} departure, {len(arrival_data)} arrival")

        except Exception as e:
            logger.error(f"❌ Error during data fetching: {str(e)}")
            logger.error(f"❌ Error type: {type(e)}")
            raise

        all_flight_data = all_flights

        # ========================================
        # 🔴 Redshift: 출발/도착 데이터 개별 조회 (레거시 - 참고용)
        # ========================================
        # try:
        #     logger.info("🛫 Fetching departure data...")
        #     departure_data = await self._fetch_departure_data(
        #         redshift_db, airport, date, scenario_id
        #     )
        #     logger.info(f"✅ Departure data fetched: {len(departure_data)} flights")
        # 
        #     logger.info("🛬 Fetching arrival data...")
        #     arrival_data = await self._fetch_arrival_data(
        #         redshift_db, airport, date, scenario_id
        #     )
        #     logger.info(f"✅ Arrival data fetched: {len(arrival_data)} flights")
        # 
        # except Exception as e:
        #     logger.error(f"❌ Error during data fetching: {str(e)}")
        #     logger.error(f"❌ Error type: {type(e)}")
        #     raise
        # 
        # all_flight_data = departure_data + arrival_data

        # 2. Extract airline mapping from flight data
        logger.info(f"🔍 Processing {len(all_flight_data)} flights for airline mapping")
        airlines = self._extract_airline_mapping(all_flight_data)
        logger.info(f"📋 Extracted {len(airlines)} airlines")

        # 3. Generate filter data
        departure_filters = self._generate_departure_filters_from_data(departure_data)
        arrival_filters = self._generate_arrival_filters_from_data(arrival_data)

        # ✅ Flight Schedule과 Passenger Schedule은 departure(출발편)만 사용
        # 따라서 total_flights는 departure 항공편 수만 반환
        total_flights = departure_filters["total_flights"]

        metadata = {
            # Request context for identification and tracking
            "airport": airport,
            "date": date,
            "scenario_id": scenario_id,
            # Flight data summary (departure flights only for consistency)
            "total_flights": total_flights,
            "filters": {"departure": departure_filters, "arrival": arrival_filters},
            "airlines": airlines,
        }

        logger.info(f"✅ Flight filters metadata generated successfully")
        logger.info(
            f"📍 Request context: {airport} on {date} (scenario: {scenario_id})"
        )
        logger.info(f"📊 Total flights: {metadata['total_flights']}")
        logger.info(f"✈️ Airlines: {len(metadata['airlines'])}")

        return metadata

    # ========================================
    # 🔵 Snowflake: 단일 쿼리로 출발/도착 모두 조회 (현재 활성)
    # ========================================
    async def _fetch_both_flights(
        self, 
        snowflake_db: Connection, 
        airport: str, 
        date: str, 
        scenario_id: str,
        conditions: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        """
        ✅ Fetch BOTH departure and arrival flight data in ONE query (optimized!)
        
        Args:
            snowflake_db: Snowflake database connection
            airport: Airport IATA code
            date: Target date (YYYY-MM-DD)
            scenario_id: Scenario ID
            conditions: Optional filter conditions dict (e.g., {"carrier": "KE", "terminal": "T2"})
        
        Returns:
            List of flight dictionaries
        """
        logger.info(f"✈️  Fetching ALL flights for {airport} on {date}")
        if conditions:
            logger.info(f"🔍 With conditions: {conditions}")

        try:
            query = SELECT_AIRPORT_FLIGHTS_BOTH
            params = {"flight_date": date, "airport": airport}
            
            logger.info(f"📅 Using flight data query (UNION structure, named params)")

            cursor = snowflake_db.cursor()
            cursor.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            cursor.close()

            flight_data = [dict(zip(columns, row)) for row in rows]
            flight_data = enrich_flight_data(flight_data)

            logger.info(f"✅ Found {len(flight_data)} total flights in ONE query (2x faster!)")
            return flight_data

        except Exception as e:
            logger.error(f"❌ Error fetching both flights: {str(e)}")
            logger.error(f"❌ Query: {query}")
            logger.error(f"❌ Params: {params}")
            return []

    # ========================================
    # 🔴 Redshift: 출발/도착 데이터 개별 조회 메서드 (레거시 - 참고용)
    # ========================================
    # async def _fetch_departure_data(
    #     self, redshift_db: Connection, airport: str, date: str, scenario_id: str
    # ) -> List[Dict[str, Any]]:
    #     """Fetch departure flight data from Redshift"""
    #     logger.info(f"🛫 Fetching departure data for {airport} on {date}")
    # 
    #     try:
    #         # Select appropriate query based on date
    #         query_date = datetime.strptime(date, "%Y-%m-%d").date()
    #         today = datetime.now().date()
    # 
    #         if query_date < today:
    #             # Historical data: flights_extended table
    #             query = SELECT_AIRPORT_FLIGHTS_EXTENDED
    #             logger.info(f"📅 Using flights_extended table for past date: {date}")
    #         else:
    #             # Current/future data: schedule table
    #             query = SELECT_AIRPORT_SCHEDULE
    #             logger.info(f"📅 Using schedule table for current/future date: {date}")
    # 
    #         # Modify query for departure flights only (like flight_schedules.py)
    #         modified_query = query.replace(
    #             "AND (fe.departure_airport_iata = %s OR fe.arrival_airport_iata = %s)",
    #             "AND fe.departure_airport_iata = %s",
    #         ).replace(
    #             "AND (s.departure_station_code_iata = %s OR s.arrival_station_code_iata = %s)",
    #             "AND s.departure_station_code_iata = %s",
    #         )
    # 
    #         # Use cursor approach like flight_schedules.py with 2 parameters
    #         cursor = redshift_db.cursor()
    #         cursor.execute(modified_query, (date, airport))  # Only 2 parameters ✅
    #         columns = [desc[0] for desc in cursor.description]
    #         rows = cursor.fetchall()
    #         cursor.close()
    # 
    #         flight_data = [dict(zip(columns, row)) for row in rows]
    # 
    #         # ✅ Remove duplicates based on carrier + flight_number + date
    #         seen = set()
    #         unique_flight_data = []
    #         for flight in flight_data:
    #             key = (
    #                 flight.get("operating_carrier_iata"),
    #                 flight.get("flight_number"),
    #                 flight.get("flight_date")
    #             )
    #             if all(key) and key not in seen:
    #                 seen.add(key)
    #                 unique_flight_data.append(flight)
    # 
    #         if len(flight_data) != len(unique_flight_data):
    #             logger.info(f"🔧 Departure duplicates removed: {len(flight_data)} → {len(unique_flight_data)} ({len(flight_data) - len(unique_flight_data)} duplicates)")
    # 
    #         logger.info(f"✅ Found {len(unique_flight_data)} unique departure flights")
    #         return unique_flight_data
    # 
    #     except Exception as e:
    #         logger.error(f"❌ Error fetching departure data: {str(e)}")
    #         return []
    # 
    # async def _fetch_arrival_data(
    #     self, redshift_db: Connection, airport: str, date: str, scenario_id: str
    # ) -> List[Dict[str, Any]]:
    #     """Fetch arrival flight data from Redshift"""
    #     logger.info(f"🛬 Fetching arrival data for {airport} on {date}")
    # 
    #     try:
    #         # Select appropriate query based on date
    #         query_date = datetime.strptime(date, "%Y-%m-%d").date()
    #         today = datetime.now().date()
    # 
    #         if query_date < today:
    #             # Historical data: flights_extended table
    #             query = SELECT_AIRPORT_FLIGHTS_EXTENDED
    #             logger.info(f"📅 Using flights_extended table for past date: {date}")
    #         else:
    #             # Current/future data: schedule table
    #             query = SELECT_AIRPORT_SCHEDULE
    #             logger.info(f"📅 Using schedule table for current/future date: {date}")
    # 
    #         # Modify query for arrival flights only (like flight_schedules.py)
    #         modified_query = query.replace(
    #             "AND (fe.departure_airport_iata = %s OR fe.arrival_airport_iata = %s)",
    #             "AND fe.arrival_airport_iata = %s",
    #         ).replace(
    #             "AND (s.departure_station_code_iata = %s OR s.arrival_station_code_iata = %s)",
    #             "AND s.arrival_station_code_iata = %s",
    #         )
    # 
    #         # Use cursor approach like flight_schedules.py with 2 parameters
    #         cursor = redshift_db.cursor()
    #         cursor.execute(modified_query, (date, airport))  # Only 2 parameters ✅
    #         columns = [desc[0] for desc in cursor.description]
    #         rows = cursor.fetchall()
    #         cursor.close()
    # 
    #         flight_data = [dict(zip(columns, row)) for row in rows]
    # 
    #         # ✅ Remove duplicates based on carrier + flight_number + date
    #         seen = set()
    #         unique_flight_data = []
    #         for flight in flight_data:
    #             key = (
    #                 flight.get("operating_carrier_iata"),
    #                 flight.get("flight_number"),
    #                 flight.get("flight_date")
    #             )
    #             if all(key) and key not in seen:
    #                 seen.add(key)
    #                 unique_flight_data.append(flight)
    # 
    #         if len(flight_data) != len(unique_flight_data):
    #             logger.info(f"🔧 Arrival duplicates removed: {len(flight_data)} → {len(unique_flight_data)} ({len(flight_data) - len(unique_flight_data)} duplicates)")
    # 
    #         logger.info(f"✅ Found {len(unique_flight_data)} unique arrival flights")
    #         return unique_flight_data
    # 
    #     except Exception as e:
    #         logger.error(f"❌ Error fetching arrival data: {str(e)}")
    #         return []

    def _extract_airline_mapping(
        self, flight_data: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        """Extract airline IATA code to name mapping from real data (simplified)"""
        airlines = {}

        # Collect unique airline mappings (take first occurrence for each IATA)
        for flight in flight_data:
            iata_code = flight.get("operating_carrier_iata")
            airline_name = flight.get("operating_carrier_name")

            if iata_code and airline_name and iata_code not in airlines:
                airlines[iata_code] = airline_name

        result = dict(sorted(airlines.items()))
        logger.info(f"📋 Extracted {len(result)} airlines from real data")

        return result

    def _generate_departure_filters_from_data(
        self, departure_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Generate departure filters from flight data"""
        filters = {}

        # ========================================
        # 🔵 PostgreSQL: 데이터가 이미 unique (현재 활성)
        # ========================================
        total_count = len(departure_data)
        filters["total_flights"] = total_count

        logger.info(f"🔍 DEBUG: Departure flights calculation")
        logger.info(f"   - Total flights: {total_count}")

        # ========================================
        # 🔴 Redshift: 중복 제거 필요 (레거시 - 참고용)
        # ========================================
        # # Calculate total unique flights FIRST (before any grouping)
        # # Use carrier + flight_number + date as uniqueness criteria
        # unique_flights = set()
        # for flight in departure_data:
        #     carrier = flight.get("operating_carrier_iata")
        #     flight_num = flight.get("flight_number")
        #     flight_date = flight.get("flight_date")
        #     if carrier and flight_num and flight_date:
        #         unique_flights.add((carrier, flight_num, flight_date))
        # 
        # total_count = len(unique_flights)
        # filters["total_flights"] = total_count
        # 
        # logger.info(f"🔍 DEBUG: Departure unique flights calculation")
        # logger.info(f"   - Total raw records: {len(departure_data)}")
        # logger.info(f"   - Unique flights (carrier+flight_number+date): {total_count}")
        # logger.info(f"   - Sample unique flights: {list(unique_flights)[:3]}")

        # 1. Group by departure terminal
        filters["departure_terminal"] = self._group_by_field(
            departure_data, "departure_terminal", "unknown"
        )

        # 2. Group by arrival region with nested countries (NEW!)
        filters["arrival_region"] = self._group_by_region_with_countries(
            departure_data, "arrival_region", "arrival_country", "Unknown"
        )

        # 3. Group by flight type
        filters["flight_type"] = self._group_by_field(
            departure_data, "flight_type", "Unknown"
        )

        logger.info(f"📊 Generated departure filters: {list(filters.keys())}, total: {total_count}")
        return filters

    def _generate_arrival_filters_from_data(
        self, arrival_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Generate arrival filters from flight data"""
        filters = {}

        # ========================================
        # 🔵 PostgreSQL: 데이터가 이미 unique (현재 활성)
        # ========================================
        total_count = len(arrival_data)
        filters["total_flights"] = total_count

        # ========================================
        # 🔴 Redshift: 중복 제거 필요 (레거시 - 참고용)
        # ========================================
        # # Calculate total unique flights FIRST (before any grouping)
        # # Use carrier + flight_number + date as uniqueness criteria
        # unique_flights = set()
        # for flight in arrival_data:
        #     carrier = flight.get("operating_carrier_iata")
        #     flight_num = flight.get("flight_number")
        #     flight_date = flight.get("flight_date")
        #     if carrier and flight_num and flight_date:
        #         unique_flights.add((carrier, flight_num, flight_date))
        # 
        # total_count = len(unique_flights)
        # filters["total_flights"] = total_count

        # 1. Group by arrival terminal
        filters["arrival_terminal"] = self._group_by_field(
            arrival_data, "arrival_terminal", "unknown"
        )

        # 2. Group by departure region with nested countries (NEW!)
        filters["departure_region"] = self._group_by_region_with_countries(
            arrival_data, "departure_region", "departure_country", "Unknown"
        )

        # 3. Group by flight type
        filters["flight_type"] = self._group_by_field(
            arrival_data, "flight_type", "Unknown"
        )

        logger.info(f"📊 Generated arrival filters: {list(filters.keys())}, total: {total_count}")
        return filters

    def _generate_flight_unique_id(self, flight: Dict[str, Any]) -> str:
        """
        항공편 고유 ID 생성 - carrier_code + flight_number 형식 (예: KE712)
        packages.flight_data.flight_number.build_flight_id 위임
        """
        from packages.flight_data.flight_number import build_flight_id
        return build_flight_id(flight) or ""

    def _group_by_field(
        self,
        flight_data: List[Dict[str, Any]],
        field_name: str,
        default_value: str = "unknown",
    ) -> Dict[str, Any]:
        """Group flight data by specified field (duplicates already removed at dataset level)"""

        # Group by field
        groups = defaultdict(list)

        for flight in flight_data:
            field_value = flight.get(field_name, default_value)
            if field_value is None or field_value == "":
                field_value = default_value
            groups[str(field_value)].append(flight)

        # Generate airline statistics for each group
        result = {}

        for field_value, flights in groups.items():
            # Re-group by airline
            airlines = defaultdict(list)

            for flight in flights:
                airline_code = flight.get("operating_carrier_iata", "XX")
                if airline_code:
                    airlines[airline_code].append(flight)

            # ========================================
            # 🔵 carrier+flight_number로 유니크 ID 생성 (현재 활성)
            # ========================================
            airline_stats = {}
            for airline_code, airline_flights in airlines.items():
                seen: set = set()
                flight_ids = []
                for flight in airline_flights:
                    fid = self._generate_flight_unique_id(flight)
                    if fid and fid not in seen:
                        seen.add(fid)
                        flight_ids.append(fid)
                airline_stats[airline_code] = {
                    "count": len(flight_ids),
                    "flight_numbers": flight_ids,
                }

            # ========================================
            # 🔴 Redshift: 중복 제거 필요 (레거시 - 참고용)
            # ========================================
            # airline_stats = {}
            # for airline_code, airline_flights in airlines.items():
            #     # 중복 제거: carrier + flight_number + date 조합으로 유니크하게 처리
            #     unique_flights = list(set([
            #         (flight.get("operating_carrier_iata"), flight.get("flight_number"), flight.get("flight_date"))
            #         for flight in airline_flights
            #         if flight.get("operating_carrier_iata") and flight.get("flight_number") and flight.get("flight_date")
            #     ]))
            # 
            #     # flight_numbers만 추출 (UI 표시용)
            #     flight_numbers = sorted(list(set([fn for _, fn, _ in unique_flights])))
            # 
            #     airline_stats[airline_code] = {
            #         "count": len(unique_flights),  # 유니크한 항공편 수
            #         "flight_numbers": flight_numbers,
            #     }

            # ✅ total_flights를 개별 항공사의 중복 제거된 count 합계로 계산
            total_count = sum(stats["count"] for stats in airline_stats.values())
            
            result[field_value] = {
                "total_flights": total_count,
                "airlines": dict(sorted(airline_stats.items())),
            }

        return result

    def _group_by_region_with_countries(
        self,
        flight_data: List[Dict[str, Any]],
        region_field: str,
        country_field: str,
        default_value: str = "Unknown",
    ) -> Dict[str, Any]:
        """
        Group flight data by region, with countries nested inside each region
        
        Returns:
        {
            "Asia": {
                "total_flights": 436,
                "countries": {
                    "China": {
                        "total_flights": 15,
                        "airlines": {...}
                    },
                    "Japan": {
                        "total_flights": 56,
                        "airlines": {...}
                    }
                }
            },
            "Europe": {...}
        }
        """
        # Step 1: Group by region
        region_groups = defaultdict(list)
        
        for flight in flight_data:
            region_value = flight.get(region_field, default_value)
            if region_value is None or region_value == "":
                region_value = default_value
            region_groups[str(region_value)].append(flight)
        
        # Step 2: For each region, create nested structure
        result = {}
        
        for region_name, region_flights in region_groups.items():
            # Step 3: Group region flights by country
            country_groups = defaultdict(list)
            for flight in region_flights:
                country_value = flight.get(country_field, default_value)
                if country_value is None or country_value == "":
                    country_value = default_value
                country_groups[str(country_value)].append(flight)
            
            # Step 4: Generate country statistics
            countries = {}
            for country_name, country_flights in country_groups.items():
                # Country level airline statistics
                country_airlines = defaultdict(list)
                for flight in country_flights:
                    airline_code = flight.get("operating_carrier_iata", "XX")
                    if airline_code:
                        country_airlines[airline_code].append(flight)
                
                # ========================================
                # 🔵 carrier+flight_number로 유니크 ID 생성 (현재 활성)
                # ========================================
                country_airline_stats = {}
                for airline_code, airline_flights in country_airlines.items():
                    seen: set = set()
                    flight_ids = []
                    for flight in airline_flights:
                        fid = self._generate_flight_unique_id(flight)
                        if fid and fid not in seen:
                            seen.add(fid)
                            flight_ids.append(fid)
                    country_airline_stats[airline_code] = {
                        "count": len(flight_ids),
                        "flight_numbers": flight_ids,
                    }

                # ========================================
                # 🔴 Redshift: 중복 제거 필요 (레거시 - 참고용)
                # ========================================
                # country_airline_stats = {}
                # for airline_code, airline_flights in country_airlines.items():
                #     # 중복 제거: carrier + flight_number + date 조합으로 유니크하게 처리
                #     unique_flights = list(set([
                #         (flight.get("operating_carrier_iata"), flight.get("flight_number"), flight.get("flight_date"))
                #         for flight in airline_flights
                #         if flight.get("operating_carrier_iata") and flight.get("flight_number") and flight.get("flight_date")
                #     ]))
                # 
                #     # flight_numbers만 추출 (UI 표시용)
                #     flight_numbers = sorted(list(set([fn for _, fn, _ in unique_flights])))
                # 
                #     country_airline_stats[airline_code] = {
                #         "count": len(unique_flights),
                #         "flight_numbers": flight_numbers,
                #     }
                
                # ✅ total_flights를 개별 항공사의 중복 제거된 count 합계로 계산
                country_total_count = sum(stats["count"] for stats in country_airline_stats.values())
                
                countries[country_name] = {
                    "total_flights": country_total_count,
                    "airlines": dict(sorted(country_airline_stats.items())),
                }
            
            # Step 5: Combine region and country data (sort countries by total_flights DESC)
            sorted_countries = dict(sorted(
                countries.items(), 
                key=lambda x: x[1]["total_flights"], 
                reverse=True  # 많은 순서부터 정렬
            ))
            
            # ✅ total_flights를 각 국가의 중복 제거된 count 합계로 계산
            region_total_count = sum(country["total_flights"] for country in countries.values())
            
            result[region_name] = {
                "total_flights": region_total_count,
                "countries": sorted_countries,
            }
        
        # Step 6: Sort regions by total_flights DESC
        sorted_result = dict(sorted(
            result.items(), 
            key=lambda x: x[1]["total_flights"], 
            reverse=True  # 많은 순서부터 정렬
        ))
        
        logger.info(f"📊 Generated region-country hierarchy for {region_field}: {len(sorted_result)} regions")
        for region_name, region_data in sorted_result.items():
            countries_count = len(region_data["countries"])
            logger.info(f"  - {region_name}: {region_data['total_flights']} flights, {countries_count} countries")
        
        return sorted_result
