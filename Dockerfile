FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

WORKDIR /app

COPY . .

RUN uv sync --frozen --no-cache

CMD [ "/app/.venv/bin/fastapi", "run", "src/main.py", "--port", "80", "--host", "0.0.0.0" ]
