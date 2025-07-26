FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

WORKDIR /app

COPY . .

RUN uv sync --frozen --no-cache

CMD [ "/app/.venv/bin/uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80" ]
