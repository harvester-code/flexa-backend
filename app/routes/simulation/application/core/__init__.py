"""
Simulation Core Business Logic

이 모듈은 시뮬레이션의 핵심 비즈니스 로직을 담고 있습니다.
복잡한 도메인 로직을 독립적인 모듈로 분리하여 유지보수성과 테스트 용이성을 향상시킵니다.
"""

from .show_up_pax import PassengerGenerator

__all__ = ["PassengerGenerator"]
