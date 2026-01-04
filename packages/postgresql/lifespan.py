"""
PostgreSQL 연결 라이프사이클 관리

이 모듈은 PostgreSQL 연결 풀의 생성과 정리를 담당합니다.
FastAPI 애플리케이션의 시작과 종료 시점에 호출되어 PostgreSQL 연결을 관리합니다.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger

from packages.postgresql.client import initialize_postgresql_pool, postgresql_connection_pool


def startup_postgresql():
    """
    PostgreSQL 연결 풀 초기화
    
    애플리케이션 시작 시점에 PostgreSQL 연결 풀을 초기화합니다.
    """
    
    logger.info("🔗 Starting PostgreSQL connection pool initialization...")
    initialize_postgresql_pool()
    logger.info("✅ PostgreSQL connection pool initialized successfully")


def shutdown_postgresql():
    """
    PostgreSQL 연결 풀 정리
    
    애플리케이션 종료 시점에 활성화된 모든 PostgreSQL 연결을 정리합니다.
    """
    
    logger.info("🔄 Starting PostgreSQL connection pool cleanup...")
    
    try:
        # psycopg3 ConnectionPool은 close() 메서드 사용
        postgresql_connection_pool.close()
        logger.info("✅ PostgreSQL connection pool closed successfully")
    except Exception as e:
        logger.error(f"❌ Error closing PostgreSQL connection pool: {e}")
        logger.info("✅ Connection pool cleanup completed with errors")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI 애플리케이션 라이프사이클 컨텍스트 매니저
    
    애플리케이션 시작과 종료 시점에서 PostgreSQL 연결 풀을
    초기화하고 정리하는 역할을 담당합니다.
    
    Args:
        app: FastAPI 애플리케이션 인스턴스
    """
    
    logger.info("🚀 Starting application with PostgreSQL services...")
    
    # === 애플리케이션 시작 단계 ===
    startup_postgresql()
    
    logger.info("✅ Application started successfully")
    
    yield  # 애플리케이션 실행
    
    # === 애플리케이션 종료 단계 ===
    logger.info("🛑 Shutting down application...")
    
    shutdown_postgresql()
    
    logger.info("✅ Application shut down successfully")

