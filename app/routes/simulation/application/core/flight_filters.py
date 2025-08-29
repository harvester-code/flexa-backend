"""
Flight Filters Metadata Processing Module

This module handles flight filtering metadata generation:
- FlightFiltersResponse: Generates filter options JSON for Departure/Arrival modes (based on real data)
"""

from typing import Dict, Any, List
from collections import defaultdict
from datetime import datetime
import pandas as pd
import json
from loguru import logger
from sqlalchemy import Connection

import awswrangler as wr
from packages.doppler.client import get_secret
from packages.aws.s3.storage import boto3_session

from ..queries import SELECT_AIRPORT_FLIGHTS_EXTENDED, SELECT_AIRPORT_SCHEDULE


class FlightFiltersResponse:
    """Flight filters metadata response generation class (based on real data)"""

    async def generate_filters_metadata(
        self, redshift_db: Connection, scenario_id: str, airport: str, date: str
    ) -> Dict[str, Any]:
        """
        Generate flight filters metadata based on real data

        Args:
            redshift_db: Redshift database connection
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

        # 1. Fetch real flight data
        try:
            logger.info("🛫 Fetching departure data...")
            departure_data = await self._fetch_departure_data(
                redshift_db, airport, date, scenario_id
            )
            logger.info(f"✅ Departure data fetched: {len(departure_data)} flights")

            logger.info("🛬 Fetching arrival data...")
            arrival_data = await self._fetch_arrival_data(
                redshift_db, airport, date, scenario_id
            )
            logger.info(f"✅ Arrival data fetched: {len(arrival_data)} flights")

        except Exception as e:
            logger.error(f"❌ Error during data fetching: {str(e)}")
            logger.error(f"❌ Error type: {type(e)}")
            raise

        all_flight_data = departure_data + arrival_data

        # 2. Extract airline mapping from flight data
        logger.info(f"🔍 Processing {len(all_flight_data)} flights for airline mapping")
        airlines = self._extract_airline_mapping(all_flight_data)
        logger.info(f"📋 Extracted {len(airlines)} airlines")

        # 3. Generate filter data
        departure_filters = self._generate_departure_filters_from_data(departure_data)
        arrival_filters = self._generate_arrival_filters_from_data(arrival_data)

        total_flights = len(all_flight_data)

        metadata = {
            # Request context for identification and tracking
            "airport": airport,
            "date": date,
            "scenario_id": scenario_id,
            # Flight data summary
            "total_flights": total_flights,
            "airlines": airlines,
            "filters": {"departure": departure_filters, "arrival": arrival_filters},
        }

        # 4. Save raw flight data to S3 (temporary)
        await self._save_flight_data_to_s3(all_flight_data, scenario_id)

        logger.info(f"✅ Flight filters metadata generated successfully")
        logger.info(
            f"📍 Request context: {airport} on {date} (scenario: {scenario_id})"
        )
        logger.info(f"📊 Total flights: {metadata['total_flights']}")
        logger.info(f"✈️ Airlines: {len(metadata['airlines'])}")
        logger.info(
            f"💾 Saved raw data to S3: s3://{get_secret('AWS_S3_BUCKET_NAME')}/{scenario_id}/flight-schedule.parquet"
        )

        return metadata

    async def _fetch_departure_data(
        self, redshift_db: Connection, airport: str, date: str, scenario_id: str
    ) -> List[Dict[str, Any]]:
        """Fetch departure flight data from Redshift"""
        logger.info(f"🛫 Fetching departure data for {airport} on {date}")

        try:
            # Select appropriate query based on date
            query_date = datetime.strptime(date, "%Y-%m-%d").date()
            today = datetime.now().date()

            if query_date < today:
                # Historical data: flights_extended table
                query = SELECT_AIRPORT_FLIGHTS_EXTENDED
                logger.info(f"📅 Using flights_extended table for past date: {date}")
            else:
                # Current/future data: schedule table
                query = SELECT_AIRPORT_SCHEDULE
                logger.info(f"📅 Using schedule table for current/future date: {date}")

            # Modify query for departure flights only (like flight_schedules.py)
            modified_query = query.replace(
                "AND (fe.departure_airport_iata = %s OR fe.arrival_airport_iata = %s)",
                "AND fe.departure_airport_iata = %s",
            ).replace(
                "AND (s.departure_station_code_iata = %s OR s.arrival_station_code_iata = %s)",
                "AND s.departure_station_code_iata = %s",
            )

            # Use cursor approach like flight_schedules.py with 2 parameters
            cursor = redshift_db.cursor()
            cursor.execute(modified_query, (date, airport))  # Only 2 parameters ✅
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            cursor.close()

            flight_data = [dict(zip(columns, row)) for row in rows]
            logger.info(f"✅ Found {len(flight_data)} departure flights")
            return flight_data

        except Exception as e:
            logger.error(f"❌ Error fetching departure data: {str(e)}")
            return []

    async def _fetch_arrival_data(
        self, redshift_db: Connection, airport: str, date: str, scenario_id: str
    ) -> List[Dict[str, Any]]:
        """Fetch arrival flight data from Redshift"""
        logger.info(f"🛬 Fetching arrival data for {airport} on {date}")

        try:
            # Select appropriate query based on date
            query_date = datetime.strptime(date, "%Y-%m-%d").date()
            today = datetime.now().date()

            if query_date < today:
                # Historical data: flights_extended table
                query = SELECT_AIRPORT_FLIGHTS_EXTENDED
                logger.info(f"📅 Using flights_extended table for past date: {date}")
            else:
                # Current/future data: schedule table
                query = SELECT_AIRPORT_SCHEDULE
                logger.info(f"📅 Using schedule table for current/future date: {date}")

            # Modify query for arrival flights only (like flight_schedules.py)
            modified_query = query.replace(
                "AND (fe.departure_airport_iata = %s OR fe.arrival_airport_iata = %s)",
                "AND fe.arrival_airport_iata = %s",
            ).replace(
                "AND (s.departure_station_code_iata = %s OR s.arrival_station_code_iata = %s)",
                "AND s.arrival_station_code_iata = %s",
            )

            # Use cursor approach like flight_schedules.py with 2 parameters
            cursor = redshift_db.cursor()
            cursor.execute(modified_query, (date, airport))  # Only 2 parameters ✅
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            cursor.close()

            flight_data = [dict(zip(columns, row)) for row in rows]
            logger.info(f"✅ Found {len(flight_data)} arrival flights")
            return flight_data

        except Exception as e:
            logger.error(f"❌ Error fetching arrival data: {str(e)}")
            return []

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

        # Total departure flights
        filters["total_flights"] = len(departure_data)

        # 1. Group by departure terminal
        filters["departure_terminal"] = self._group_by_field(
            departure_data, "departure_terminal", "unknown"
        )

        # 2. Group by arrival region
        filters["arrival_region"] = self._group_by_field(
            departure_data, "arrival_region", "Unknown"
        )

        # 3. Group by arrival country
        filters["arrival_country"] = self._group_by_field(
            departure_data, "arrival_country", "Unknown"
        )

        # 4. Group by flight type
        filters["flight_type"] = self._group_by_field(
            departure_data, "flight_type", "Unknown"
        )

        logger.info(f"📊 Generated departure filters: {list(filters.keys())}")
        return filters

    def _generate_arrival_filters_from_data(
        self, arrival_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Generate arrival filters from flight data"""
        filters = {}

        # Total arrival flights
        filters["total_flights"] = len(arrival_data)

        # 1. Group by arrival terminal
        filters["arrival_terminal"] = self._group_by_field(
            arrival_data, "arrival_terminal", "unknown"
        )

        # 2. Group by departure region
        filters["departure_region"] = self._group_by_field(
            arrival_data, "departure_region", "Unknown"
        )

        # 3. Group by departure country
        filters["departure_country"] = self._group_by_field(
            arrival_data, "departure_country", "Unknown"
        )

        # 4. Group by flight type
        filters["flight_type"] = self._group_by_field(
            arrival_data, "flight_type", "Unknown"
        )

        logger.info(f"📊 Generated arrival filters: {list(filters.keys())}")
        return filters

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

            # Generate airline statistics (no additional deduplication needed)
            airline_stats = {}
            for airline_code, airline_flights in airlines.items():
                flight_numbers = [
                    flight.get("flight_number", 0)
                    for flight in airline_flights
                    if flight.get("flight_number")
                ]

                airline_stats[airline_code] = {
                    "count": len(airline_flights),
                    "flight_numbers": sorted(flight_numbers),
                }

            result[field_value] = {
                "total_flights": len(flights),  # Already deduplicated at dataset level
                "airlines": dict(sorted(airline_stats.items())),
            }

        return result

    async def _save_flight_data_to_s3(
        self, flight_data: List[Dict[str, Any]], scenario_id: str
    ):
        """Save raw flight data to S3 as parquet file (temporary)"""
        if not flight_data:
            logger.warning("Empty flight data, skipping S3 save")
            return

        try:
            # Convert flight data list to DataFrame and save as parquet
            wr.s3.to_parquet(
                df=pd.DataFrame(flight_data),
                path=f"s3://{get_secret('AWS_S3_BUCKET_NAME')}/{scenario_id}/flight-schedule.parquet",
                boto3_session=boto3_session,
            )

            logger.info(
                f"💾 Raw flight data saved to S3 as parquet: {len(flight_data)} flights"
            )

        except Exception as e:
            logger.error(f"❌ Failed to save flight data to S3: {str(e)}")
            # API should still respond normally even if S3 save fails
            pass
