FROM ghcr.io/astral-sh/uv:0.7.2 AS uv

FROM public.ecr.aws/lambda/python:3.13 AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_NO_INSTALLER_METADATA=1 \
    UV_LINK_MODE=copy

RUN --mount=from=uv,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv export --frozen --no-emit-workspace --no-dev --no-editable -o requirements.txt && \
    uv pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

FROM public.ecr.aws/lambda/python:3.13

RUN dnf swap -y curl-minimal curl-full && \
    dnf swap -y gnupg2-minimal gnupg2-full && \
    curl -Ls --tlsv1.2 --proto "=https" --retry 3 https://cli.doppler.com/install.sh | sh

COPY --from=builder ${LAMBDA_TASK_ROOT} ${LAMBDA_TASK_ROOT}

COPY ./src ${LAMBDA_TASK_ROOT}/app

ENTRYPOINT [ "doppler", "run", "--" ]
CMD [ "app.main.handler" ]
