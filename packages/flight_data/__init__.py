"""
Flight Data Provider - 환경변수 기반 데이터소스 스위치

FLIGHT_DATA_SOURCE 환경변수로 데이터소스를 전환합니다:
  - "postgresql" (기본값): 기존 PostgreSQL 파이프라인
  - "snowflake": OAG Snowflake 파이프라인

사용법:
  from packages.flight_data import get_snowflake_connection, SELECT_AIRPORT_FLIGHTS_BOTH, lifespan
  from packages.flight_data import enrich_flight_data  # Snowflake 국가/지역 보강
"""

from packages.doppler.client import get_secret
from loguru import logger

FLIGHT_DATA_SOURCE = get_secret("FLIGHT_DATA_SOURCE", "postgresql")
logger.info(f"Flight data source configured: {FLIGHT_DATA_SOURCE}")

if FLIGHT_DATA_SOURCE == "snowflake":
    from packages.snowflake.client import get_snowflake_connection
    from packages.snowflake.queries import SELECT_AIRPORT_FLIGHTS_BOTH
    from packages.snowflake.lifespan import lifespan
else:
    from packages.postgresql.client import get_postgresql_connection as get_snowflake_connection
    from packages.postgresql.queries import SELECT_AIRPORT_FLIGHTS_BOTH
    from packages.postgresql.lifespan import lifespan

from packages.flight_data.enrichment import enrich_flight_data
from packages.flight_data.flight_number import normalize_flight_number, build_flight_id, build_flight_id_from_row

__all__ = [
    "get_snowflake_connection",
    "SELECT_AIRPORT_FLIGHTS_BOTH",
    "lifespan",
    "FLIGHT_DATA_SOURCE",
    "enrich_flight_data",
    "normalize_flight_number",
    "build_flight_id",
    "build_flight_id_from_row",
]
