import logging
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from packages.doppler.client import get_secret

# SQLAlchemy 모델 베이스
Base = declarative_base()

# 로깅 설정
logger = logging.getLogger(__name__)

# ============================================================
# 환경별 DB 연결 설정 최적화 (개발 환경 장시간 사용 지원)
# ============================================================
# Supabase DB 연결 정보 (Doppler secrets에서 가져옴)
SUPABASE_USERNAME = get_secret("SUPABASE_USERNAME")
SUPABASE_PASSWORD = get_secret("SUPABASE_PASSWORD")
SUPABASE_HOST = get_secret("SUPABASE_HOST")
SUPABASE_PORT = get_secret("SUPABASE_PORT")
SUPABASE_DBNAME = get_secret("SUPABASE_DBNAME")

# 환경 감지 (개발 환경 여부 확인)
ENVIRONMENT = get_secret("DOPPLER_ENVIRONMENT")
IS_DEV = ENVIRONMENT == "dev"

# 환경별 연결 풀 설정
if IS_DEV:
    # 개발 환경: 장시간 사용을 위한 최적화
    POOL_SETTINGS = {
        "pool_size": 25,
        "max_overflow": 15,
        "pool_timeout": 60,
        "pool_recycle": 3600,      # 1시간마다 연결 재활용
        "pool_pre_ping": True,
        "connect_args": {
            "timeout": 60,
            "command_timeout": 120,
            "server_settings": {
                "application_name": "flexa_dev",
                "jit": "off"       # JIT 비활성화로 개발시 성능 안정화
            }
        }
    }
elif ENVIRONMENT == "stg":
    # 스테이징 환경: 안정성 중시
    POOL_SETTINGS = {
        "pool_size": 20,
        "max_overflow": 10,
        "pool_timeout": 30,
        "pool_recycle": 1800,      # 30분마다 재활용
        "pool_pre_ping": True,
        "connect_args": {
            "timeout": 45,
            "command_timeout": 90,
            "server_settings": {
                "application_name": "flexa_stg"
            }
        }
    }
else:
    # 운영 환경 (prod): 리소스 효율성 중시
    POOL_SETTINGS = {
        "pool_size": 15,
        "max_overflow": 5,
        "pool_timeout": 20,
        "pool_recycle": 900,       # 15분마다 재활용
        "pool_pre_ping": True,
        "connect_args": {
            "timeout": 30,
            "command_timeout": 60,
            "server_settings": {
                "application_name": "flexa_prod"
            }
        }
    }

# 최적화된 DB 엔진 생성
SUPABASE_ENGINE = create_async_engine(
    f"postgresql+asyncpg://{SUPABASE_USERNAME}:{SUPABASE_PASSWORD}@{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DBNAME}",
    **POOL_SETTINGS,
    echo=IS_DEV,      # 개발 환경에서만 SQL 로그 출력
    echo_pool=IS_DEV  # 개발 환경에서만 풀 로그 출력
)

AsyncSessionLocal = sessionmaker(
    bind=SUPABASE_ENGINE,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def aget_supabase_session() -> AsyncGenerator[AsyncSession, None]:
    """최적화된 Supabase DB 세션 생성기
    
    개발 환경에서 장시간 사용을 위한 안정적인 세션 관리:
    - 자동 롤백 처리
    - 연결 상태 모니터링  
    - 에러 로깅 추가
    """
    session = None
    try:
        session = AsyncSessionLocal()
        
        # 개발 환경에서만 연결 상태 로깅
        if IS_DEV:
            pool = SUPABASE_ENGINE.pool
            logger.debug(f"DB Pool Status - Size: {pool.size()}, Checked out: {pool.checkedout()}")
            
        yield session
        
        # 트랜잭션 커밋 (세션이 정상적으로 사용된 경우)
        await session.commit()
        
    except Exception as e:
        if session:
            await session.rollback()
            logger.error(f"Database session error: {str(e)}")
        raise
    finally:
        if session:
            await session.close()
            if IS_DEV:
                logger.debug("Database session closed successfully")
