# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS build
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock ./
COPY src ./src
RUN uv sync --frozen --all-extras

FROM python:3.13-slim-bookworm
RUN groupadd -r sigbot && useradd -r -g sigbot sigbot && mkdir -p /app/logs && chown sigbot:sigbot /app/logs
WORKDIR /app
COPY --from=build --chown=sigbot:sigbot /app/.venv /app/.venv
COPY --chown=sigbot:sigbot src ./src
COPY --chown=sigbot:sigbot sig-api/mapping.json ./sig-api/mapping.json
COPY --chown=sigbot:sigbot migrations ./migrations
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
USER sigbot
CMD ["sigbot"]
