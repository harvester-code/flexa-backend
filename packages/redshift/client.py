import asyncio
import time

import redshift_connector
from fastapi import HTTPException
from loguru import logger
from sqlalchemy.pool import QueuePool

from packages.doppler.client import get_secret

# ============================================================
# NOTE: Redshift 연결을 위한 설정 (Refactored for production)
POOL_RECYCLE = 60 * 60 * 6  # 6 hours
POOL_SIZE_MAP = {"development": 5, "production": 20}
POOL_SIZE = POOL_SIZE_MAP.get(get_secret("ENVIRONMENT"), 1)
TIMEOUT = 60


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


# Recycle connection if it exceeds POOL_RECYCLE
def recycle_wrapper(conn):
    age = time.time() - getattr(conn, "_created_at", 0)
    if age > POOL_RECYCLE:
        logger.warning("Connection exceeded POOL_RECYCLE. Recycling entire pool.")
        try:
            conn.close()
        except Exception:
            pass
        # 🔥 Reset the entire pool
        try:
            redshift_pool.dispose()
        except Exception as e:
            logger.error(f"Error disposing pool: {e}")
        raise Exception("Expired connection recycled. Pool reset.")
    return conn


# Actual connection creator (includes recycle check)
def redshift_connect_recycled():
    conn = redshift_connect()
    return recycle_wrapper(conn)


# Create a SQLAlchemy QueuePool for Redshift connections
redshift_pool = QueuePool(
    redshift_connect,
    max_overflow=10,  # Allow some overflow connections (for temporary spikes)
    pool_size=POOL_SIZE,
    timeout=TIMEOUT,
)


# Validate Redshift connection
async def validate_redshift_connection(conn):
    """Validate a Redshift connection by executing a simple query."""
    try:
        await asyncio.to_thread(conn.cursor().execute, "SELECT 1")
        return True
    except redshift_connector.Error as e:
        logger.error(f"Connection validation failed: {e}")
        return False


# Get Redshift connection
async def get_redshift_connection():
    conn = None
    try:
        # Retrieve a connection from the pool
        conn = await asyncio.to_thread(redshift_pool.connect)
        try:
            # Recycle the connection if it exceeds POOL_RECYCLE
            recycle_wrapper(conn)

            logger.info("✅ Redshift connection recycled")
        except Exception:
            # Try to recreate the connection
            conn = await asyncio.to_thread(redshift_pool.connect)

        # Validate the connection
        if not await validate_redshift_connection(conn):
            conn.close()
            raise HTTPException(status_code=500, detail="Invalid Redshift connection")

        yield conn
    except Exception as e:
        logger.error(f"Error acquiring Redshift connection: {e}")
        raise HTTPException(
            status_code=503, detail="Error acquiring Redshift connection"
        )
    finally:
        if conn:
            try:
                conn.close()  # Return the connection to the pool
            except Exception as e:
                logger.error(f"Error returning Redshift connection to pool: {e}")


# Connection pool reference for lifespan management
redshift_connection_pool = redshift_pool


# Initialize Redshift connection pool
def initialize_redshift_pool():
    """Initialize Redshift connection pool on application startup."""
    logger.info("🔗 Initializing Redshift connection pool...")
    logger.info(f"Pool size: {POOL_SIZE}, Max overflow: 10, Timeout: {TIMEOUT}s")
