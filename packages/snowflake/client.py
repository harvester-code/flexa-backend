import snowflake.connector
from fastapi import HTTPException
from loguru import logger

from packages.doppler.client import get_secret

TIMEOUT = 30


def get_snowflake_config():
    """Snowflake 연결 설정 반환 (Doppler secrets에서 환경변수 사용)"""
    account = get_secret("SNOWFLAKE_ACCOUNT")
    user = get_secret("SNOWFLAKE_USER")
    password = get_secret("SNOWFLAKE_PASSWORD")
    database = get_secret("SNOWFLAKE_DATABASE", "OAG_SCHEDULES")
    schema = get_secret("SNOWFLAKE_SCHEMA", "DIRECT_CUSTOMER_CONFIGURATIONS")
    warehouse = get_secret("SNOWFLAKE_WAREHOUSE", "GENERAL")
    role = get_secret("SNOWFLAKE_ROLE", "PUBLIC")

    if not all([account, user, password]):
        missing = [k for k, v in {
            "SNOWFLAKE_ACCOUNT": account,
            "SNOWFLAKE_USER": user,
            "SNOWFLAKE_PASSWORD": password,
        }.items() if not v]
        raise ValueError(f"Missing required Snowflake environment variables: {', '.join(missing)}")

    return {
        "account": account,
        "user": user,
        "password": password,
        "database": database,
        "schema": schema,
        "warehouse": warehouse,
        "role": role,
        "login_timeout": TIMEOUT,
        "network_timeout": TIMEOUT,
    }


def create_snowflake_connection():
    """Snowflake 연결 생성"""
    config = get_snowflake_config()
    return snowflake.connector.connect(**config)


async def get_snowflake_connection():
    """Snowflake 연결 가져오기 (FastAPI Dependency) - PostgreSQL get_postgresql_connection과 동일 인터페이스"""
    conn = None
    try:
        conn = create_snowflake_connection()
        yield conn

    except snowflake.connector.errors.DatabaseError as e:
        logger.error(f"Snowflake database error: {type(e).__name__}: {e}")
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        raise HTTPException(status_code=503, detail="Snowflake connection error")

    except Exception as e:
        logger.error(f"Error with Snowflake connection: {type(e).__name__}: {e}")
        if conn:
            try:
                conn.close()
            except Exception:
                pass

        if "timeout" in str(e).lower():
            raise HTTPException(status_code=504, detail="Snowflake connection timeout")
        else:
            raise HTTPException(status_code=500, detail="Snowflake connection error")

    finally:
        if conn:
            try:
                conn.close()
                logger.debug("Snowflake connection closed")
            except Exception as e:
                logger.error(f"Error closing Snowflake connection: {e}")


def initialize_snowflake():
    """Snowflake 연결 초기화 검증"""
    logger.info("Initializing Snowflake connection...")
    config = get_snowflake_config()
    logger.info(f"Account: {config['account']}, Database: {config['database']}, "
                f"Schema: {config['schema']}, Warehouse: {config['warehouse']}")

    try:
        conn = create_snowflake_connection()
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION()")
        version = cur.fetchone()[0]
        cur.close()
        conn.close()
        logger.info(f"Snowflake connection verified. Version: {version}")
    except Exception as e:
        logger.error(f"Failed to verify Snowflake connection: {e}")
        raise


def shutdown_snowflake():
    """Snowflake 정리 (Snowflake는 요청별 연결이므로 별도 풀 정리 불필요)"""
    logger.info("Snowflake cleanup complete")
