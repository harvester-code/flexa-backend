import asyncio
import time

import redshift_connector
from fastapi import HTTPException
from loguru import logger
from sqlalchemy.pool import QueuePool

from packages.doppler.client import get_secret

# ============================================================
# NOTE: Redshift 연결을 위한 설정 (Optimized for AWS Redshift idle timeouts)

# 동적으로 조정 가능한 POOL_RECYCLE (테스트용)
_DEFAULT_POOL_RECYCLE = 60 * 15  # 15분으로 단축 (AWS Redshift idle timeout 대응)
_TEST_POOL_RECYCLE = None  # 테스트용 오버라이드

def get_pool_recycle_time():
    """현재 POOL_RECYCLE 시간을 반환 (테스트 모드 고려)"""
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

POOL_RECYCLE = get_pool_recycle_time()  # 동적으로 계산됨
POOL_SIZE_MAP = {"development": 3, "production": 10, "dev": 3, "stg": 5, "prod": 10}
TIMEOUT = 20  # 연결 대기시간 더 단축
MAX_RETRIES = 3  # 연결 실패 시 재시도 횟수


def get_environment_pool_size():
    """환경별 Pool Size를 안전하게 감지"""
    env = get_secret("DOPPLER_ENVIRONMENT")
    logger.info(f"🔍 Detected environment: '{env}'")
    
    if env in POOL_SIZE_MAP:
        size = POOL_SIZE_MAP[env]
    else:
        # 기본값을 더 안전하게 설정
        env_lower = str(env).lower() if env else "unknown"
        if any(keyword in env_lower for keyword in ['dev', 'development']):
            size = 5
        elif any(keyword in env_lower for keyword in ['prod', 'production']):
            size = 20
        else:
            size = 3  # 안전한 기본값
        logger.warning(f"Unknown environment '{env}', using default pool size: {size}")
    
    logger.info(f"✅ Pool size set to: {size}")
    return size


POOL_SIZE = get_environment_pool_size()


# Redshift Connection
def redshift_connect():
    conn = redshift_connector.connect(
        host=get_secret("REDSHIFT_HOST"),
        database=get_secret("REDSHIFT_DBNAME"),
        port=get_secret("REDSHIFT_PORT"),
        user=get_secret("REDSHIFT_USERNAME"),
        password=get_secret("REDSHIFT_PASSWORD"),
    )
    conn._created_at = time.time()  # Save creation time
    return conn


# Recycle connection if it exceeds POOL_RECYCLE (동적 계산)
def recycle_wrapper(conn):
    current_recycle_time = get_pool_recycle_time()
    age = time.time() - getattr(conn, "_created_at", 0)
    if age > current_recycle_time:
        logger.warning(f"Connection exceeded POOL_RECYCLE ({age:.0f}s > {current_recycle_time}s). Closing individual connection.")
        try:
            conn.close()  # 개별 연결만 닫기 (풀은 유지)
        except Exception as e:
            logger.error(f"Error closing expired connection: {e}")
        raise Exception("Expired connection closed. Pool maintained.")
    return conn


# Create an optimized SQLAlchemy QueuePool for AWS Redshift
redshift_pool = QueuePool(
    redshift_connect,  # 기본 연결 함수 (우리의 커스텀 재활용 로직 사용)
    max_overflow=5,  # 낮춤: pool이 차면 빠르게 fresh connection 생성
    pool_size=POOL_SIZE,
    timeout=TIMEOUT,
    # Note: pool_recycle과 pool_pre_ping은 이 버전에서 지원되지 않음
    # 대신 우리의 recycle_wrapper와 validation 함수에서 처리
)


# Enhanced Redshift connection validation with retry logic
async def validate_redshift_connection(conn):
    """Enhanced connection validation with proper cleanup and timeout."""
    cursor = None
    try:
        # Quick timeout for validation to prevent hanging
        cursor = conn.cursor()
        await asyncio.wait_for(
            asyncio.to_thread(cursor.execute, "SELECT 1"), 
            timeout=5.0  # 5초 validation timeout
        )
        result = await asyncio.to_thread(cursor.fetchone)
        return result is not None
    except asyncio.TimeoutError:
        logger.error("Connection validation timeout - connection may be stale")
        return False
    except redshift_connector.Error as e:
        logger.error(f"Redshift connection validation failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected validation error: {e}")
        return False
    finally:
        # ✅ Critical: 커서 정리로 semaphore 누수 방지
        if cursor:
            try:
                cursor.close()
            except Exception as e:
                logger.debug(f"Cursor cleanup error (non-critical): {e}")


# Robust connection creation with retry logic
async def create_fresh_connection(retries=MAX_RETRIES):
    """Create a new connection with retry logic for network issues."""
    for attempt in range(retries):
        try:
            logger.info(f"🔗 Creating fresh connection (attempt {attempt + 1}/{retries})")
            conn = await asyncio.wait_for(
                asyncio.to_thread(redshift_connect),
                timeout=10.0  # 10초 connection timeout
            )
            
            # Validate the new connection
            if await validate_redshift_connection(conn):
                logger.info("✅ Fresh connection created and validated")
                return conn
            else:
                logger.warning("❌ New connection failed validation, closing")
                conn.close()
                
        except asyncio.TimeoutError:
            logger.warning(f"Connection timeout on attempt {attempt + 1}")
        except Exception as e:
            logger.error(f"Connection creation failed on attempt {attempt + 1}: {e}")
        
        # Wait before retry (exponential backoff)
        if attempt < retries - 1:
            wait_time = 2 ** attempt  # 1s, 2s, 4s
            logger.info(f"⏳ Waiting {wait_time}s before retry...")
            await asyncio.sleep(wait_time)
    
    raise Exception(f"Failed to create connection after {retries} attempts")


# Enhanced Redshift connection with robust error handling
async def get_redshift_connection():
    conn = None
    fresh_connection_created = False
    
    try:
        # Try to get connection from pool first
        try:
            conn = await asyncio.wait_for(
                asyncio.to_thread(redshift_pool.connect), 
                timeout=10.0
            )
            
            # Check if connection needs recycling
            try:
                recycle_wrapper(conn)
            except Exception as e:
                if "Expired connection closed" in str(e):
                    logger.info("♻️ Connection expired, creating fresh one")
                    if conn:
                        try:
                            conn.close()
                        except:
                            pass
                    # Create fresh connection with retry logic
                    conn = await create_fresh_connection()
                    fresh_connection_created = True
                else:
                    raise e
            
            # Validate connection if not fresh (fresh connections are already validated)
            if not fresh_connection_created:
                if not await validate_redshift_connection(conn):
                    logger.warning("♻️ Pool connection failed validation, creating fresh one")
                    conn.close()
                    conn = await create_fresh_connection()
                    fresh_connection_created = True
                    
        except asyncio.TimeoutError:
            logger.warning("Pool connection timeout, creating fresh connection")
            conn = await create_fresh_connection()
            fresh_connection_created = True
            
        yield conn
        
    except Exception as e:
        logger.error(f"Error acquiring Redshift connection: {type(e).__name__}: {e}")
        if conn:
            try:
                conn.close()
            except:
                pass
                
        # Determine appropriate HTTP status code
        if "timeout" in str(e).lower():
            raise HTTPException(status_code=504, detail="Database connection timeout")
        elif "network" in str(e).lower() or "socket" in str(e).lower():
            raise HTTPException(status_code=503, detail="Database connection unavailable")
        else:
            raise HTTPException(status_code=500, detail="Database connection error")
            
    finally:
        # Enhanced cleanup
        if conn:
            try:
                # Clean up any cursors
                if hasattr(conn, '_cursors') and conn._cursors:
                    for cursor in list(conn._cursors):
                        try:
                            cursor.close()
                        except:
                            pass
                conn.close()
                if fresh_connection_created:
                    logger.debug("🧹 Fresh connection cleaned up")
            except Exception as e:
                logger.error(f"Error in connection cleanup: {e}")


# Connection pool reference for lifespan management
redshift_connection_pool = redshift_pool


# Enhanced pool status and monitoring functions
def get_pool_status() -> dict:
    """Get current pool status for debugging/monitoring"""
    try:
        status = {
            "pool_size": redshift_pool.size(),
            "checked_in": redshift_pool.checkedin(),
            "checked_out": redshift_pool.checkedout(), 
            "overflow": redshift_pool.overflow(),
            "pool_recycle_seconds": POOL_RECYCLE,
            "total_connections": redshift_pool.size() + redshift_pool.overflow()
        }
        
        # 연결 풀 건강 상태 평가
        total_capacity = status["pool_size"] + 5  # max_overflow
        utilization = (status["checked_out"] + status["overflow"]) / total_capacity * 100
        
        status["utilization_percent"] = round(utilization, 1)
        status["health_status"] = "healthy" if utilization < 80 else "warning" if utilization < 95 else "critical"
        
        return status
    except Exception as e:
        logger.error(f"Error getting pool status: {e}")
        return {"error": str(e)}


def log_pool_metrics():
    """24/7 모니터링을 위한 풀 상태 로깅"""
    status = get_pool_status()
    if "error" not in status:
        logger.info(f"📊 Pool Metrics: size={status['pool_size']}, "
                   f"active={status['checked_out']}, idle={status['checked_in']}, "
                   f"overflow={status['overflow']}, utilization={status['utilization_percent']}%, "
                   f"health={status['health_status']}")
    else:
        logger.error(f"Failed to get pool metrics: {status['error']}")



# Initialize Redshift connection pool
def initialize_redshift_pool():
    """Initialize optimized Redshift connection pool on application startup."""
    logger.info("🔗 Initializing Optimized Redshift connection pool for AWS...")
    logger.info(f"Pool size: {POOL_SIZE}, Max overflow: 5, Timeout: {TIMEOUT}s")
    logger.info(f"Pool recycle: {POOL_RECYCLE}s ({POOL_RECYCLE/60:.1f} minutes)")
    logger.info(f"Max retries: {MAX_RETRIES}, Enhanced validation: True")
    logger.info("🛡️ Features: Connection aging, Retry logic, Timeout protection")
    
    # 초기 풀 상태 로깅
    log_pool_metrics()
