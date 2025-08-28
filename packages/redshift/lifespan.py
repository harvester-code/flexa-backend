"""
Redshift 연결 라이프사이클 관리

이 모듈은 Redshift 연결 풀의 생성과 정리를 담당합니다.
FastAPI 애플리케이션의 시작과 종료 시점에 호출되어 Redshift 연결을 관리합니다.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger

from packages.redshift.client import initialize_redshift_pool, redshift_connection_pool


def startup_redshift():
    """
    Redshift 연결 풀 초기화

    애플리케이션 시작 시점에 Redshift 연결 풀을 초기화합니다.
    """

    logger.info("🔗 Starting Redshift connection pool initialization...")
    initialize_redshift_pool()
    logger.info("✅ Redshift connection pool initialized successfully")


def shutdown_redshift():
    """
    Redshift 연결 풀 정리

    애플리케이션 종료 시점에 활성화된 모든 Redshift 연결을 정리합니다.
    """

    logger.info("🔄 Starting Redshift connection pool cleanup...")

    try:
        # SQLAlchemy QueuePool의 올바른 정리 방법
        redshift_connection_pool.dispose()
        logger.info("✅ Redshift connection pool disposed successfully")
    except Exception as e:
        logger.error(f"❌ Error disposing Redshift connection pool: {e}")
        logger.info("✅ Connection pool cleanup completed with errors")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI 애플리케이션 라이프사이클 컨텍스트 매니저

    애플리케이션 시작과 종료 시점에서 Redshift 연결 풀을
    초기화하고 정리하는 역할을 담당합니다.

    Args:
        app: FastAPI 애플리케이션 인스턴스
    """

    logger.info("🚀 Starting application with Redshift services...")

    # === 애플리케이션 시작 단계 ===
    startup_redshift()

    logger.info("✅ Application started successfully")

    yield  # 애플리케이션 실행

    # === 애플리케이션 종료 단계 ===
    logger.info("🛑 Shutting down application...")

    shutdown_redshift()

    logger.info("✅ Application shut down successfully")
