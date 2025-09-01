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
POOL_SIZE_MAP = {"development": 3, "production": 10, "dev": 3, "prod": 10, "local": 2}
TIMEOUT = 20  # 연결 대기시간 더 단축
MAX_RETRIES = 3  # 연결 실패 시 재시도 횟수


def get_environment_pool_size():
    """환경별 Pool Size를 안전하게 감지"""
    env = get_secret("ENVIRONMENT")
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


# Actual connection creator (includes recycle check)
def redshift_connect_recycled():
    conn = redshift_connect()
    return recycle_wrapper(conn)


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
            "test_mode": TEST_MODE if 'TEST_MODE' in globals() else False,
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


# Test connection with forced aging
async def test_connection_with_aging():
    """테스트용: 연결 생성 후 강제로 만료시켜 재활용 로직 테스트"""
    try:
        logger.info("🧪 Testing connection aging and recycling logic...")
        
        # 1. 정상 연결 획득
        conn = await asyncio.to_thread(redshift_pool.connect)
        logger.info(f"✅ Initial connection acquired at {time.time()}")
        
        # 2. 연결 시간을 과거로 조작
        old_time = time.time() - POOL_RECYCLE - 10  # 10초 더 과거
        conn._created_at = old_time
        logger.info(f"🧪 Connection age manipulated to {old_time} (expired)")
        
        # 3. 연결 반납 (풀로 되돌림)
        conn.close()
        
        # 4. 다시 연결 획득 시도 (재활용 로직 실행)
        logger.info("🧪 Attempting to get connection again (should trigger recycle)")
        
        test_conn = None
        recycle_triggered = False
        try:
            test_conn = await asyncio.to_thread(redshift_pool.connect)
            recycle_wrapper(test_conn)  # 수동으로 recycle 체크
            logger.info("✅ Connection acquired after recycle check")
        except Exception as e:
            if "Expired connection closed" in str(e):
                recycle_triggered = True
                logger.info("✅ Recycle logic triggered successfully!")
                # 재활용 후 새 연결 시도
                test_conn = await asyncio.to_thread(redshift_pool.connect)
                logger.info("✅ New connection acquired after recycling")
            else:
                raise e
        finally:
            if test_conn:
                test_conn.close()
        
        return {
            "message": "Connection aging test completed",
            "recycle_triggered": recycle_triggered,
            "pool_status": get_pool_status()
        }
        
    except Exception as e:
        logger.error(f"Connection aging test failed: {e}")
        return {
            "message": "Connection aging test failed",
            "error": str(e),
            "pool_status": get_pool_status()
        }


# ============================================================
# 추가 테스트 함수들 (즉시 테스트용)

async def test_connection_breakdown():
    """연결을 강제로 끊어서 복구 로직 테스트"""
    try:
        logger.info("🧪 Testing connection breakdown and recovery...")
        
        # 1. 정상 연결 획득
        conn = await asyncio.to_thread(redshift_pool.connect)
        logger.info("✅ Normal connection acquired")
        
        # 2. 연결을 강제로 닫기 (broken pipe 시뮬레이션)
        try:
            conn.close()
            logger.info("🔌 Connection forcibly closed")
        except Exception as e:
            logger.debug(f"Error closing connection: {e}")
        
        # 3. 새 연결 시도 (복구 테스트)
        logger.info("🔄 Attempting to get new connection...")
        recovery_start = time.time()
        
        try:
            new_conn = None
            async with get_redshift_connection() as test_conn:
                new_conn = test_conn
                cursor = test_conn.cursor()
                cursor.execute("SELECT 'Recovery successful' as message")
                result = cursor.fetchone()
                cursor.close()
                
                recovery_time = time.time() - recovery_start
                logger.info(f"✅ Connection recovered in {recovery_time:.2f}s")
                
                return {
                    "message": "Connection breakdown and recovery test completed",
                    "recovery_successful": True,
                    "recovery_time_seconds": round(recovery_time, 2),
                    "test_result": result[0] if result else None,
                    "pool_status": get_pool_status()
                }
                
        except Exception as e:
            recovery_time = time.time() - recovery_start
            logger.error(f"❌ Recovery failed: {e}")
            return {
                "message": "Connection recovery test failed",
                "recovery_successful": False,
                "recovery_time_seconds": round(recovery_time, 2),
                "error": str(e),
                "pool_status": get_pool_status()
            }
        
    except Exception as e:
        logger.error(f"Connection breakdown test failed: {e}")
        return {
            "message": "Connection breakdown test failed",
            "error": str(e),
            "pool_status": get_pool_status()
        }


async def test_pool_exhaustion(concurrent_requests=10):
    """Pool exhaustion 테스트 - 동시에 많은 연결 요청"""
    try:
        logger.info(f"🧪 Testing pool exhaustion with {concurrent_requests} concurrent requests...")
        
        start_time = time.time()
        results = []
        
        async def single_connection_test(request_id):
            try:
                request_start = time.time()
                async with get_redshift_connection() as conn:
                    cursor = conn.cursor()
                    # 짧은 쿼리 실행
                    cursor.execute(f"SELECT {request_id} as request_id, CURRENT_TIMESTAMP")
                    result = cursor.fetchone()
                    cursor.close()
                    
                    request_time = time.time() - request_start
                    return {
                        "request_id": request_id,
                        "success": True,
                        "time": round(request_time, 2),
                        "result": str(result[1]) if result else None
                    }
            except Exception as e:
                request_time = time.time() - request_start
                return {
                    "request_id": request_id,
                    "success": False,
                    "time": round(request_time, 2),
                    "error": str(e)
                }
        
        # 모든 요청을 동시에 실행
        tasks = [single_connection_test(i) for i in range(concurrent_requests)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 결과 분석
        total_time = time.time() - start_time
        successful = sum(1 for r in results if isinstance(r, dict) and r.get('success'))
        failed = len(results) - successful
        avg_time = sum(r.get('time', 0) for r in results if isinstance(r, dict)) / len(results)
        
        logger.info(f"✅ Pool exhaustion test completed: {successful}/{len(results)} successful")
        
        return {
            "message": "Pool exhaustion test completed",
            "concurrent_requests": concurrent_requests,
            "successful": successful,
            "failed": failed,
            "total_time_seconds": round(total_time, 2),
            "average_request_time": round(avg_time, 2),
            "results": results[:5],  # 처음 5개 결과만 표시
            "pool_status_after": get_pool_status()
        }
        
    except Exception as e:
        logger.error(f"Pool exhaustion test failed: {e}")
        return {
            "message": "Pool exhaustion test failed",
            "error": str(e),
            "pool_status": get_pool_status()
        }


async def test_rapid_recycle(recycle_seconds=30):
    """빠른 재활용 테스트 - POOL_RECYCLE을 짧게 설정하고 테스트"""
    try:
        logger.info(f"🧪 Testing rapid recycle with {recycle_seconds}s recycle time...")
        
        # 1. 기존 POOL_RECYCLE 저장
        original_recycle = get_pool_recycle_time()
        
        # 2. 테스트용 짧은 시간 설정
        set_test_pool_recycle(recycle_seconds)
        
        try:
            # 3. 연결 생성
            conn = await asyncio.to_thread(redshift_pool.connect)
            creation_time = time.time()
            logger.info(f"📅 Connection created at {creation_time}")
            
            # 4. 연결 반납 (pool로 돌아감)
            conn.close()
            logger.info(f"🔄 Connection returned to pool")
            
            # 5. 재활용 시간까지 대기
            wait_time = recycle_seconds + 5  # 5초 여유
            logger.info(f"⏳ Waiting {wait_time}s for connection to expire...")
            await asyncio.sleep(wait_time)
            
            # 6. 새 연결 요청 (재활용 로직 실행됨)
            logger.info("🔄 Requesting new connection (should trigger recycle)...")
            test_start = time.time()
            
            async with get_redshift_connection() as new_conn:
                cursor = new_conn.cursor()
                cursor.execute("SELECT 'Recycle test successful' as message")
                result = cursor.fetchone()
                cursor.close()
                
                test_time = time.time() - test_start
                logger.info(f"✅ Rapid recycle test completed in {test_time:.2f}s")
                
                return {
                    "message": "Rapid recycle test completed",
                    "recycle_seconds": recycle_seconds,
                    "wait_time": wait_time,
                    "test_time_seconds": round(test_time, 2),
                    "test_result": result[0] if result else None,
                    "recycle_triggered": True,  # 재활용이 실행되었다고 가정
                    "pool_status": get_pool_status()
                }
                
        finally:
            # 7. 원래 POOL_RECYCLE 복원
            reset_pool_recycle()
            logger.info(f"🔄 POOL_RECYCLE restored to {original_recycle}s")
        
    except Exception as e:
        # 에러 발생 시도 원래 설정 복원
        reset_pool_recycle()
        logger.error(f"Rapid recycle test failed: {e}")
        return {
            "message": "Rapid recycle test failed",
            "error": str(e),
            "pool_status": get_pool_status()
        }


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
    
    if 'TEST_MODE' in globals() and TEST_MODE:
        logger.info("🧪 Running in TEST MODE - pool recycle can be overridden")
