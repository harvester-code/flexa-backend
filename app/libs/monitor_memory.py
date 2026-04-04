import os
import threading
import time

import psutil
from loguru import logger

from packages.doppler.client import get_secret


def monitor_memory():
    """
    현재 프로세스의 메모리 사용량을 모니터링하고, 의미 있는 변화가 있을 때만 로그에 기록합니다.
    
    로그 출력 조건:
    - 메모리 사용량이 이전 *로그 출력 시점* 대비 10% 이상 변화했을 때
    - 500MB / 1GB 경계를 새로 넘었을 때 (이미 넘어있으면 반복 출력 안 함)
    """

    process = psutil.Process(os.getpid())
    last_logged_mb: float = 0.0
    was_above_500 = False
    was_above_1g = False
    check_interval = 5

    while True:
        rss_mb = process.memory_info().rss / (1024**2)

        should_log = False
        log_level = "info"
        message = ""

        above_1g = rss_mb > 1024
        above_500 = rss_mb > 500

        if above_1g and not was_above_1g:
            should_log = True
            log_level = "warning"
            message = f"[Memory] RSS crossed 1GB: {rss_mb:.0f} MB"
        elif above_500 and not was_above_500:
            should_log = True
            message = f"[Memory] RSS crossed 500MB: {rss_mb:.0f} MB"
        elif not above_500 and was_above_500:
            should_log = True
            message = f"[Memory] RSS dropped below 500MB: {rss_mb:.0f} MB"
        elif last_logged_mb > 0:
            change_pct = abs(rss_mb - last_logged_mb) / last_logged_mb * 100
            if change_pct >= 10:
                direction = "increased" if rss_mb > last_logged_mb else "decreased"
                should_log = True
                message = f"[Memory] RSS {direction} {change_pct:.0f}% ({last_logged_mb:.0f} → {rss_mb:.0f} MB)"
        else:
            should_log = True
            message = f"[Memory] Monitoring started. RSS: {rss_mb:.0f} MB"

        if should_log:
            if log_level == "warning":
                logger.warning(message)
            else:
                logger.info(message)
            last_logged_mb = rss_mb

        was_above_500 = above_500
        was_above_1g = above_1g
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
