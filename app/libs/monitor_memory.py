import os
import threading
import time

import psutil
from loguru import logger

from packages.doppler.client import get_secret


def monitor_memory():
    """
    현재 프로세스의 메모리 사용량을 모니터링하고, 특별한 변화가 있을 때만 로그에 기록합니다.
    
    로그 출력 조건:
    - 메모리 사용량이 이전 값 대비 10% 이상 증가했을 때
    - 메모리 사용량이 500MB를 초과했을 때
    - 메모리 사용량이 1GB를 초과했을 때 (경고)
    
    RSS (Resident Set Size)는 프로세스가 실제로 물리적 메모리(RAM)에 점유하고 있는 메모리 크기를 의미합니다.
    이는 프로세스가 사용하는 전체 메모리 중에서 디스크 스왑 영역이 아닌 실제 메모리에 상주하는 부분을 나타냅니다.
    """

    process = psutil.Process(os.getpid())
    previous_rss_mb = None
    check_interval = 5  # 5초마다 체크
    
    while True:
        mem_info = process.memory_info()
        rss_mb = mem_info.rss / (1024**2)  # Convert bytes to MB
        
        should_log = False
        log_level = "info"
        message = ""
        
        # 1. 메모리 사용량이 1GB를 초과한 경우 (경고)
        if rss_mb > 1024:
            should_log = True
            log_level = "warning"
            message = f"[Memory Monitor] ⚠️  High memory usage detected! RSS: {rss_mb:.2f} MB (>1GB)"
        
        # 2. 메모리 사용량이 500MB를 초과한 경우
        elif rss_mb > 500:
            should_log = True
            log_level = "info"
            message = f"[Memory Monitor] 📊 Memory usage above 500MB. RSS: {rss_mb:.2f} MB"
        
        # 3. 이전 값과 비교하여 10% 이상 증가한 경우
        elif previous_rss_mb is not None:
            increase_percent = ((rss_mb - previous_rss_mb) / previous_rss_mb) * 100
            if increase_percent >= 10:
                should_log = True
                log_level = "info"
                message = f"[Memory Monitor] 📈 Memory usage increased by {increase_percent:.1f}% ({previous_rss_mb:.2f} MB → {rss_mb:.2f} MB)"
        
        # 4. 이전 값이 없을 때는 첫 로그만 출력 (초기 상태 확인)
        elif previous_rss_mb is None:
            should_log = True
            log_level = "info"
            message = f"[Memory Monitor] 🚀 Memory monitoring started. Initial RSS: {rss_mb:.2f} MB"
        
        # 로그 출력
        if should_log:
            if log_level == "warning":
                logger.warning(message)
            else:
                logger.info(message)
        
        previous_rss_mb = rss_mb
        time.sleep(check_interval)


def setup_memory_monitor():
    """
    메모리 모니터링을 설정합니다. 이 함수는 모니터링 스레드를 시작합니다.
    """

    if get_secret("DOPPLER_ENVIRONMENT") == "dev":
        threading.Thread(target=monitor_memory, daemon=True).start()
        logger.info("Memory monitoring started.")
    else:
        logger.info("Memory monitoring is disabled in non-dev environments.")
