# StreamPulse producer image. Versions follow docs/COMPATIBILITY.md (pinned, >= 2 weeks old).
FROM ghcr.io/astral-sh/uv:0.13.0 AS uv

FROM python:3.14.8-slim-trixie

COPY --from=uv /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencies first, in their own layer: rebuilt only when uv.lock changes.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY README.md ./
COPY src ./src
RUN uv sync --locked --no-dev

# Run as an unprivileged user; the producer needs no filesystem writes except recordings.
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin producer
USER producer

EXPOSE 8000
ENTRYPOINT ["streampulse-producer"]
CMD ["--mode", "live"]
