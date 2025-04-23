ARG FUNCTION_DIR="/function"

FROM python:3.11-bookworm AS builder

ARG FUNCTION_DIR

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

RUN pip install poetry==1.8.5

WORKDIR ${FUNCTION_DIR}

COPY pyproject.toml poetry.lock ./

RUN --mount=type=cache,target=$POETRY_CACHE_DIR poetry install --no-root

FROM python:3.11-slim-bookworm AS runtime

ARG FUNCTION_DIR

ENV PYTHON_VENV=${FUNCTION_DIR}/.venv \
    PATH="${FUNCTION_DIR}/.venv/bin:$PATH"

RUN apt-get update && apt-get install -y apt-transport-https ca-certificates curl gnupg && \
    curl -sLf --retry 3 --tlsv1.2 --proto "=https" 'https://packages.doppler.com/public/cli/gpg.DE2A7741A397C129.key' | gpg --dearmor -o /usr/share/keyrings/doppler-archive-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/doppler-archive-keyring.gpg] https://packages.doppler.com/public/cli/deb/debian any-version main" | tee /etc/apt/sources.list.d/doppler-cli.list && \
    apt-get update && \
    apt-get -y install doppler

WORKDIR ${FUNCTION_DIR}

COPY --from=builder ${PYTHON_VENV} ${PYTHON_VENV}

COPY . .

ENTRYPOINT [ "doppler", "run", "--", "python", "-m", "awslambdaric" ]
CMD [ "src.main.handler" ]
