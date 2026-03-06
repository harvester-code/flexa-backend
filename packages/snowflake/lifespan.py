"""
Snowflake 연결 라이프사이클 관리

PostgreSQL lifespan.py와 동일한 인터페이스.
FastAPI 애플리케이션의 시작과 종료 시점에 호출되어 Snowflake 연결을 검증/정리합니다.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger

from packages.snowflake.client import initialize_snowflake, shutdown_snowflake


def startup_snowflake_service():
    logger.info("Starting Snowflake service initialization...")
    initialize_snowflake()
    logger.info("Snowflake service initialized successfully")


def shutdown_snowflake_service():
    logger.info("Starting Snowflake service cleanup...")
    shutdown_snowflake()
    logger.info("Snowflake service shut down successfully")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting application with Snowflake services...")

    startup_snowflake_service()

    logger.info("Application started successfully")

    yield

    logger.info("Shutting down application...")

    shutdown_snowflake_service()

    logger.info("Application shut down successfully")
