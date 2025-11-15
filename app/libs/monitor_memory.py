import os
import threading
import time

import psutil
from loguru import logger

from packages.doppler.client import get_secret


def monitor_memory():
    """
    현재 프로세스의 메모리 사용량을 모니터링하고 5초마다 RSS(Resident Set Size)를 메가바이트 단위로 로그에 기록합니다.

    RSS (Resident Set Size)는 프로세스가 실제로 물리적 메모리(RAM)에 점유하고 있는 메모리 크기를 의미합니다.
    이는 프로세스가 사용하는 전체 메모리 중에서 디스크 스왑 영역이 아닌 실제 메모리에 상주하는 부분을 나타냅니다.
    """

    process = psutil.Process(os.getpid())
    while True:
        mem_info = process.memory_info()
        rss_mb = mem_info.rss / (1024**2)  # Convert bytes to MB
        logger.info(f"[Memory Monitor] RSS: {rss_mb:.2f} MB")
        time.sleep(5)


def setup_memory_monitor():
    """
    메모리 모니터링을 설정합니다. 이 함수는 모니터링 스레드를 시작합니다.
    """

    if get_secret("DOPPLER_ENVIRONMENT") == "dev":
        threading.Thread(target=monitor_memory, daemon=True).start()
        logger.info("Memory monitoring started.")
    else:
        logger.info("Memory monitoring is disabled in non-dev environments.")
