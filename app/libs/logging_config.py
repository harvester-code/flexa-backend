import logging
import sys

from loguru import logger

# 참고: https://medium.com/@muh.bazm/how-i-unified-logging-in-fastapi-with-uvicorn-and-loguru-6813058c48fc


class InterceptHandler(logging.Handler):
    def emit(self, record):
        logger_opt = logger.opt(depth=6, exception=record.exc_info)
        logger_opt.log(record.levelname, record.getMessage())


def setup_logging():
    # 기존 로거 초기화 (root 포함)
    logging.root.handlers = [InterceptHandler()]
    logging.root.setLevel(logging.INFO)

    # uvicorn 관련 로거들도 리디렉션
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"):
        logging.getLogger(name).handlers = [InterceptHandler()]
        logging.getLogger(name).propagate = False

    # Loguru 기본 출력 설정
    logger.remove()
    logger.add(
        sys.stdout,
        level="INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>",
        enqueue=True,  # 멀티 프로세스 지원
        backtrace=True,
        diagnose=False,  # True면 전체 소스코드 추적됨 (개발용)
    )
