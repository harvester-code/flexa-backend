FROM python:3.13-slim

# 시스템 의존성 설치
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# UV 설치 (pip을 통한 설치)
RUN pip install uv

# 작업 디렉토리 설정
WORKDIR /app

# Python 의존성 파일 복사
COPY pyproject.toml uv.lock* ./

# UV로 의존성 설치 (--frozen 옵션 제거)
RUN uv sync

# 애플리케이션 코드 복사
COPY . .

# 포트 설정
EXPOSE 8000

# FastAPI 실행 (개발 모드 - 자동 리로드)
CMD ["uv", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"] 