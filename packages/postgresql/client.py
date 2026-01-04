import asyncio
import time

import psycopg
from fastapi import HTTPException
from loguru import logger
from psycopg_pool import ConnectionPool

from packages.doppler.client import get_secret

# ============================================================
# NOTE: PostgreSQL 연결을 위한 설정 (psycopg3 사용)

_DEFAULT_POOL_RECYCLE = 60 * 15  # 15분
_TEST_POOL_RECYCLE = None

def get_pool_recycle_time():
    """현재 POOL_RECYCLE 시간을 반환"""
    if _TEST_POOL_RECYCLE is not None:
        return _TEST_POOL_RECYCLE
    return _DEFAULT_POOL_RECYCLE

def set_test_pool_recycle(seconds):
    """테스트용 POOL_RECYCLE 시간 설정"""
    global _TEST_POOL_RECYCLE
    _TEST_POOL_RECYCLE = seconds
    logger.info(f"🧪 TEST: POOL_RECYCLE set to {seconds}s ({seconds/60:.1f} minutes)")

def reset_pool_recycle():
    """POOL_RECYCLE을 기본값으로 복원"""
    global _TEST_POOL_RECYCLE
    _TEST_POOL_RECYCLE = None
    logger.info(f"🔄 POOL_RECYCLE reset to default {_DEFAULT_POOL_RECYCLE}s")

POOL_SIZE_MAP = {"development": 3, "production": 10, "dev": 3, "stg": 5, "prod": 10}
TIMEOUT = 20
MAX_RETRIES = 3


def get_environment_pool_size():
    """환경별 Pool Size를 안전하게 감지"""
    env = get_secret("DOPPLER_ENVIRONMENT")
    logger.info(f"🔍 Detected environment: '{env}'")
    
    if env in POOL_SIZE_MAP:
        size = POOL_SIZE_MAP[env]
    else:
        env_lower = str(env).lower() if env else "unknown"
        if any(keyword in env_lower for keyword in ['dev', 'development']):
            size = 5
        elif any(keyword in env_lower for keyword in ['prod', 'production']):
            size = 20
        else:
            size = 3
        logger.warning(f"Unknown environment '{env}', using default pool size: {size}")
    
    logger.info(f"✅ Pool size set to: {size}")
    return size


POOL_SIZE = get_environment_pool_size()


# PostgreSQL 연결 문자열 생성
def get_postgresql_conninfo():
    """PostgreSQL 연결 문자열 반환 (Doppler secrets에서 환경변수 사용)"""
    host = get_secret("POSTGRES_HOST")
    port = get_secret("POSTGRES_PORT", "5432")
    dbname = get_secret("POSTGRES_DB")
    user = get_secret("POSTGRES_USER")
    password = get_secret("POSTGRES_PASSWORD")
    sslmode = get_secret("POSTGRES_SSLMODE", "disable")
    
    # 필수 환경변수 검증
    if not all([host, dbname, user, password]):
        missing = [k for k, v in {
            "POSTGRES_HOST": host,
            "POSTGRES_DB": dbname,
            "POSTGRES_USER": user,
            "POSTGRES_PASSWORD": password
        }.items() if not v]
        raise ValueError(f"Missing required PostgreSQL environment variables: {', '.join(missing)}")
    
    return f"""
        host={host}
        port={port}
        dbname={dbname}
        user={user}
        password={password}
        sslmode={sslmode}
    """.strip()


# Create PostgreSQL Connection Pool (psycopg3)
postgresql_pool = ConnectionPool(
    conninfo=get_postgresql_conninfo(),
    min_size=1,
    max_size=POOL_SIZE + 5,
    timeout=TIMEOUT,
    max_lifetime=get_pool_recycle_time(),  # 연결 최대 수명
)


# Connection validation
async def validate_postgresql_connection(conn):
    """PostgreSQL 연결 검증"""
    cursor = None
    try:
        cursor = conn.cursor()
        await asyncio.wait_for(
            asyncio.to_thread(cursor.execute, "SELECT 1"), 
            timeout=5.0
        )
        result = await asyncio.to_thread(cursor.fetchone)
        return result is not None
    except asyncio.TimeoutError:
        logger.error("Connection validation timeout")
        return False
    except psycopg.Error as e:
        logger.error(f"PostgreSQL connection validation failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected validation error: {e}")
        return False
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception as e:
                logger.debug(f"Cursor cleanup error: {e}")


# Enhanced PostgreSQL connection with robust error handling
async def get_postgresql_connection():
    """PostgreSQL 연결 가져오기 (FastAPI Dependency)"""
    conn = None
    
    try:
        # Get connection from pool
        conn = await asyncio.to_thread(postgresql_pool.getconn)
        
        # Validate connection
        if not await validate_postgresql_connection(conn):
            logger.warning("⚠️ Connection validation failed, reconnecting...")
            postgresql_pool.putconn(conn)
            conn = await asyncio.to_thread(postgresql_pool.getconn)
        
        yield conn
        
    except Exception as e:
        logger.error(f"Error acquiring PostgreSQL connection: {type(e).__name__}: {e}")
        
        # Rollback on error
        if conn:
            try:
                await asyncio.to_thread(conn.rollback)
            except:
                pass
        
        if "timeout" in str(e).lower():
            raise HTTPException(status_code=504, detail="Database connection timeout")
        elif "network" in str(e).lower() or "socket" in str(e).lower():
            raise HTTPException(status_code=503, detail="Database connection unavailable")
        else:
            raise HTTPException(status_code=500, detail="Database connection error")
            
    finally:
        # Commit and return connection to pool
        if conn:
            try:
                # 읽기 전용 쿼리도 트랜잭션을 종료해야 함 (psycopg3는 기본적으로 트랜잭션 시작)
                try:
                    await asyncio.to_thread(conn.commit)
                except Exception:
                    # 이미 종료된 트랜잭션이면 무시
                    pass
                postgresql_pool.putconn(conn)
                logger.debug("🔄 Connection returned to pool")
            except Exception as e:
                logger.error(f"Error returning connection to pool: {e}")


# Connection pool reference for lifespan management
postgresql_connection_pool = postgresql_pool


# Pool status and monitoring
def get_pool_status() -> dict:
    """현재 풀 상태 조회"""
    try:
        status = {
            "pool_size": POOL_SIZE,
            "pool_recycle_seconds": get_pool_recycle_time(),
            "health_status": "healthy"
        }
        return status
    except Exception as e:
        logger.error(f"Error getting pool status: {e}")
        return {"error": str(e)}


def log_pool_metrics():
    """풀 상태 로깅"""
    status = get_pool_status()
    if "error" not in status:
        logger.info(f"📊 Pool Metrics: size={status['pool_size']}, health={status['health_status']}")
    else:
        logger.error(f"Failed to get pool metrics: {status['error']}")


# Initialize PostgreSQL connection pool
def initialize_postgresql_pool():
    """PostgreSQL 연결 풀 초기화"""
    logger.info("🔗 Initializing PostgreSQL connection pool...")
    logger.info(f"Pool size: {POOL_SIZE}, Max connections: {POOL_SIZE + 5}, Timeout: {TIMEOUT}s")
    logger.info(f"Pool recycle: {get_pool_recycle_time()}s ({get_pool_recycle_time()/60:.1f} minutes)")
    logger.info("🛡️ Features: Connection pooling, Timeout protection, Auto-reconnect")
    
    log_pool_metrics()
