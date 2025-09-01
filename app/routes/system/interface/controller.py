from fastapi import APIRouter, status
from app.libs.response import SuccessResponse
from packages.redshift.client import (
    get_pool_status, 
    test_connection_with_aging,
    get_redshift_connection,
    log_pool_metrics,
    test_connection_breakdown,
    test_pool_exhaustion,
    test_rapid_recycle,
    set_test_pool_recycle,
    reset_pool_recycle,
    get_pool_recycle_time
)

system_router = APIRouter()


@system_router.get(
    "/health",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="애플리케이션 헬스체크 엔드포인트",
    description="API 서버의 상태를 확인하는 헬스체크 엔드포인트입니다.",
    tags=["System"],
)
async def health_check():
    """
    헬스체크 엔드포인트

    애플리케이션이 정상적으로 동작하는지 확인합니다.
    """
    return SuccessResponse(
        status_code=status.HTTP_200_OK,
    )


@system_router.get(
    "/redshift/pool-status",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="Redshift 연결 풀 상태 확인",
    description="현재 Redshift 연결 풀의 상태를 확인합니다. 디버깅 및 모니터링용입니다.",
    tags=["System", "Debug"],
)
async def get_redshift_pool_status():
    """
    Redshift 연결 풀 상태 확인
    
    연결 풀의 현재 상태와 통계를 반환합니다.
    """
    pool_status = get_pool_status()
    return SuccessResponse(
        status_code=status.HTTP_200_OK,
        data=pool_status
    )


@system_router.post(
    "/redshift/test-connection",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="Redshift 연결 테스트",
    description="실제 Redshift 연결을 테스트하고 연결 재활용 로직을 확인합니다.",
    tags=["System", "Debug"],
)
async def test_redshift_connection():
    """
    Redshift 연결 테스트
    
    실제 연결을 획득하고 해제하여 연결 풀 동작을 테스트합니다.
    """
    try:
        # async generator를 올바르게 사용
        connection_gen = get_redshift_connection()
        conn = await connection_gen.__anext__()
        
        try:
            # 간단한 쿼리 실행
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_TIMESTAMP")
            result = cursor.fetchone()
            cursor.close()
            
            return SuccessResponse(
                status_code=status.HTTP_200_OK,
                data={
                    "message": "Connection test successful",
                    "timestamp": str(result[0]) if result else None,
                    "pool_status": get_pool_status()
                }
            )
        finally:
            # 연결 정리
            try:
                await connection_gen.__anext__()
            except StopAsyncIteration:
                pass
    except Exception as e:
        return SuccessResponse(
            status_code=status.HTTP_200_OK,
            data={
                "message": "Connection test failed", 
                "error": str(e),
                "pool_status": get_pool_status()
            }
        )


@system_router.post(
    "/redshift/test-aging",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="연결 만료 및 재활용 테스트 (테스트용)",
    description="실제 연결을 만료시키고 재활용 로직이 올바르게 동작하는지 테스트합니다.",
    tags=["System", "Debug"],
)
async def test_redshift_aging():
    """
    연결 만료 및 재활용 테스트
    
    실제 연결을 강제로 만료시키고 새 연결 획득이 정상 동작하는지 확인합니다.
    """
    result = await test_connection_with_aging()
    return SuccessResponse(
        status_code=status.HTTP_200_OK,
        data=result
    )


@system_router.get(
    "/redshift/health-comprehensive",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="24시간 무중단 운영을 위한 종합 헬스체크",
    description="연결 풀 상태, 연결 테스트, 시스템 리소스를 종합적으로 확인하여 24시간 운영 준비 상태를 점검합니다.",
    tags=["System", "Health"],
)
async def comprehensive_health_check():
    """
    24시간 무중단 운영을 위한 종합 헬스체크
    
    - 연결 풀 상태 및 utilization 체크
    - 실제 연결 및 쿼리 실행 테스트
    - 시스템 리소스 상태 확인
    - 권장사항 제공
    """
    import gc
    import os
    
    checks = {}
    recommendations = []
    
    try:
        # 1. Pool 상태 체크
        pool_status = get_pool_status()
        checks['pool_healthy'] = pool_status.get('health_status') in ['healthy', 'warning']
        checks['pool_status'] = pool_status
        
        if pool_status.get('health_status') == 'critical':
            recommendations.append("Pool utilization이 높습니다. 연결 수를 늘리거나 요청 최적화를 고려하세요.")
        
        # 2. 연결 테스트
        try:
            connection_gen = get_redshift_connection()
            conn = await connection_gen.__anext__()
            
            try:
                # 간단한 쿼리 실행
                cursor = conn.cursor()
                cursor.execute("SELECT CURRENT_TIMESTAMP, version()")
                result = cursor.fetchone()
                cursor.close()
                
                checks['connection_healthy'] = True
                checks['connection_timestamp'] = str(result[0]) if result else None
                checks['redshift_version'] = str(result[1])[:50] + "..." if result and len(str(result[1])) > 50 else str(result[1]) if result else None
            finally:
                try:
                    await connection_gen.__anext__()
                except StopAsyncIteration:
                    pass
                    
        except Exception as e:
            checks['connection_healthy'] = False
            checks['connection_error'] = str(e)
            recommendations.append("연결 테스트에 실패했습니다. Redshift 서비스 상태를 확인하세요.")
        
        # 3. 시스템 리소스 체크
        try:
            try:
                import psutil
                process = psutil.Process(os.getpid())
                memory_info = process.memory_info()
                cpu_percent = process.cpu_percent()
                
                checks['memory_mb'] = round(memory_info.rss / 1024 / 1024, 1)
                checks['cpu_percent'] = cpu_percent
                checks['memory_healthy'] = memory_info.rss < 1024 * 1024 * 1024  # 1GB 미만
                checks['cpu_healthy'] = cpu_percent < 80
                
                if not checks['memory_healthy']:
                    recommendations.append("메모리 사용량이 높습니다. 메모리 누수를 확인하세요.")
                if not checks['cpu_healthy']:
                    recommendations.append("CPU 사용량이 높습니다. 시스템 부하를 확인하세요.")
            except ImportError:
                checks['memory_mb'] = "psutil not available"
                checks['cpu_percent'] = "psutil not available"
                checks['memory_healthy'] = True  # psutil 없으면 건너뜀
                checks['cpu_healthy'] = True
        except Exception as e:
            checks['system_resource_error'] = str(e)
            checks['memory_healthy'] = True  # 기본값
            checks['cpu_healthy'] = True
        
        # 4. 가비지 컬렉션 상태
        gc_count = len(gc.get_objects())
        checks['gc_objects'] = gc_count
        checks['gc_healthy'] = gc_count < 100000
        
        if not checks['gc_healthy']:
            recommendations.append("객체 수가 많습니다. 메모리 정리가 필요할 수 있습니다.")
        
        # 5. 종합 평가
        critical_checks = ['pool_healthy', 'connection_healthy']
        important_checks = ['memory_healthy', 'cpu_healthy', 'gc_healthy']
        
        critical_healthy = all(checks.get(check, False) for check in critical_checks)
        important_healthy = all(checks.get(check, True) for check in important_checks)
        
        overall_healthy = critical_healthy and important_healthy
        
        # 6. 권장사항 추가
        if overall_healthy:
            recommendations.append("✅ 시스템이 24시간 무중단 운영에 적합한 상태입니다.")
        else:
            if not critical_healthy:
                recommendations.append("⚠️ 중요한 문제가 발견되었습니다. 즉시 조치가 필요합니다.")
            if not important_healthy:
                recommendations.append("⚡ 성능 개선이 필요한 부분이 있습니다.")
        
        # Pool metrics 로깅
        log_pool_metrics()
        
        return SuccessResponse(
            status_code=status.HTTP_200_OK,
            data={
                'overall_healthy': overall_healthy,
                'critical_healthy': critical_healthy,
                'performance_healthy': important_healthy,
                'checks': checks,
                'recommendations': recommendations,
                'timestamp': checks.get('connection_timestamp'),
                'summary': f"Pool: {pool_status.get('health_status', 'unknown')}, "
                          f"Memory: {checks['memory_mb']}MB, "
                          f"Objects: {checks['gc_objects']}"
            }
        )
        
    except Exception as e:
        return SuccessResponse(
            status_code=status.HTTP_200_OK,
            data={
                'overall_healthy': False,
                'error': str(e),
                'recommendations': ['시스템에 예상치 못한 오류가 발생했습니다. 로그를 확인하세요.'],
                'checks': checks
            }
        )


# ============================================================
# 새로운 즉시 테스트 엔드포인트들

@system_router.post(
    "/redshift/test-breakdown",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="연결 끊김 및 복구 테스트 (즉시 실행)",
    description="연결을 강제로 끊고 복구 로직이 정상 동작하는지 즉시 테스트합니다.",
    tags=["System", "Debug", "Instant-Test"],
)
async def test_redshift_breakdown():
    """연결 끊김 및 복구 테스트 - 즉시 실행"""
    result = await test_connection_breakdown()
    return SuccessResponse(
        status_code=status.HTTP_200_OK,
        data=result
    )


@system_router.post(
    "/redshift/test-load",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="Pool 부하 테스트 (동시 연결)",
    description="여러 동시 연결 요청으로 pool exhaustion을 테스트합니다.",
    tags=["System", "Debug", "Load-Test"],
)
async def test_redshift_load(concurrent_requests: int = 8):
    """Pool 부하 테스트 - 동시 연결 요청"""
    if concurrent_requests > 20:
        concurrent_requests = 20  # 안전 제한
    
    result = await test_pool_exhaustion(concurrent_requests)
    return SuccessResponse(
        status_code=status.HTTP_200_OK,
        data=result
    )


@system_router.post(
    "/redshift/test-rapid-recycle",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="빠른 연결 재활용 테스트 (30초 대기)",
    description="POOL_RECYCLE을 30초로 설정하고 연결 재활용을 빠르게 테스트합니다.",
    tags=["System", "Debug", "Instant-Test"],
)
async def test_redshift_rapid_recycle(recycle_seconds: int = 30):
    """빠른 연결 재활용 테스트"""
    if recycle_seconds < 10:
        recycle_seconds = 10  # 최소 10초
    if recycle_seconds > 300:
        recycle_seconds = 300  # 최대 5분
    
    result = await test_rapid_recycle(recycle_seconds)
    return SuccessResponse(
        status_code=status.HTTP_200_OK,
        data=result
    )


@system_router.get(
    "/redshift/recycle-time",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="현재 POOL_RECYCLE 시간 확인",
    description="현재 설정된 POOL_RECYCLE 시간을 확인합니다.",
    tags=["System", "Debug"],
)
async def get_current_recycle_time():
    """현재 POOL_RECYCLE 시간 확인"""
    current_time = get_pool_recycle_time()
    return SuccessResponse(
        status_code=status.HTTP_200_OK,
        data={
            "pool_recycle_seconds": current_time,
            "pool_recycle_minutes": round(current_time / 60, 1),
            "is_test_mode": current_time != 900,  # 900초 (15분)가 기본값
            "default_seconds": 900
        }
    )


@system_router.post(
    "/redshift/set-recycle-time",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="POOL_RECYCLE 시간 동적 변경 (테스트용)",
    description="테스트를 위해 POOL_RECYCLE 시간을 동적으로 변경합니다. 주의: 운영 환경에서 사용 금지!",
    tags=["System", "Debug", "Test-Only"],
)
async def set_recycle_time(seconds: int):
    """POOL_RECYCLE 시간 동적 변경 (테스트용)"""
    if seconds < 10:
        return SuccessResponse(
            status_code=status.HTTP_200_OK,
            data={
                "success": False,
                "message": "Minimum recycle time is 10 seconds",
                "current_time": get_pool_recycle_time()
            }
        )
    
    if seconds > 3600:  # 1시간 최대
        seconds = 3600
    
    set_test_pool_recycle(seconds)
    return SuccessResponse(
        status_code=status.HTTP_200_OK,
        data={
            "success": True,
            "message": f"POOL_RECYCLE set to {seconds}s ({seconds/60:.1f} minutes)",
            "previous_time": 900,  # 기본값
            "new_time": seconds,
            "warning": "⚠️ This is for testing only! Don't use in production!"
        }
    )


@system_router.post(
    "/redshift/reset-recycle-time",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="POOL_RECYCLE 시간 기본값으로 복원",
    description="테스트용으로 변경된 POOL_RECYCLE 시간을 기본값(15분)으로 복원합니다.",
    tags=["System", "Debug"],
)
async def reset_recycle_time():
    """POOL_RECYCLE 시간 기본값으로 복원"""
    reset_pool_recycle()
    return SuccessResponse(
        status_code=status.HTTP_200_OK,
        data={
            "success": True,
            "message": "POOL_RECYCLE reset to default (15 minutes)",
            "current_time": get_pool_recycle_time(),
            "current_minutes": get_pool_recycle_time() / 60
        }
    )


@system_router.get(
    "/redshift/test-suite",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="모든 Redshift 테스트 목록",
    description="사용 가능한 모든 Redshift 연결 테스트 목록과 설명을 반환합니다.",
    tags=["System", "Debug"],
)
async def get_test_suite():
    """사용 가능한 모든 테스트 목록"""
    tests = {
        "instant_tests": [
            {
                "name": "Connection Breakdown Test",
                "endpoint": "POST /api/v1/system/redshift/test-breakdown",
                "description": "연결을 강제로 끊고 복구 테스트 (즉시 실행)",
                "duration": "~5초"
            },
            {
                "name": "Pool Load Test",
                "endpoint": "POST /api/v1/system/redshift/test-load?concurrent_requests=8",
                "description": "동시 연결 요청으로 pool 부하 테스트",
                "duration": "~10초"
            },
            {
                "name": "Aging Test (Original)",
                "endpoint": "POST /api/v1/system/redshift/test-aging",
                "description": "연결 수명을 조작해서 재활용 로직 테스트",
                "duration": "~5초"
            }
        ],
        "timed_tests": [
            {
                "name": "Rapid Recycle Test",
                "endpoint": "POST /api/v1/system/redshift/test-rapid-recycle?recycle_seconds=30",
                "description": "30초 대기 후 연결 재활용 테스트",
                "duration": "~35초"
            },
            {
                "name": "Custom Recycle Test",
                "setup": "POST /api/v1/system/redshift/set-recycle-time (10-3600초)",
                "test": "실제 요청 후 대기",
                "cleanup": "POST /api/v1/system/redshift/reset-recycle-time",
                "description": "원하는 시간으로 설정하고 실제 대기 테스트",
                "duration": "설정 시간에 따라"
            }
        ],
        "monitoring": [
            {
                "name": "Pool Status",
                "endpoint": "GET /api/v1/system/redshift/pool-status",
                "description": "현재 연결 풀 상태 확인"
            },
            {
                "name": "Comprehensive Health",
                "endpoint": "GET /api/v1/system/redshift/health-comprehensive",
                "description": "종합 헬스체크 (연결+시스템)"
            },
            {
                "name": "Connection Test",
                "endpoint": "POST /api/v1/system/redshift/test-connection",
                "description": "단순 연결 테스트"
            }
        ]
    }
    
    return SuccessResponse(
        status_code=status.HTTP_200_OK,
        data={
            "message": "🧪 Redshift Connection Test Suite",
            "current_pool_recycle": f"{get_pool_recycle_time()}s ({get_pool_recycle_time()/60:.1f}min)",
            "available_tests": tests,
            "quick_start": [
                "1. POST /test-breakdown (즉시 연결 끊김 테스트)",
                "2. POST /test-rapid-recycle?recycle_seconds=30 (30초 대기 테스트)",
                "3. POST /test-load?concurrent_requests=10 (부하 테스트)"
            ]
        }
    )
