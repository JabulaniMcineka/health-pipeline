# =============================================================================
# Dockerfile – Health Pipeline
# Runs the dbt project against a PostgreSQL backend.
# Compatible with the provided docker-compose.yml.
# =============================================================================

FROM python:3.11-slim AS base

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        curl \
        libpq-dev \
        gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
ENV POETRY_VERSION=1.8.3 \
    POETRY_HOME=/opt/poetry \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1

RUN curl -sSL https://install.python-poetry.org | python3 - \
    && ln -s /opt/poetry/bin/poetry /usr/local/bin/poetry

WORKDIR /app

# ---------------------------------------------------------------------------
# Dependencies layer (cached unless pyproject.toml / poetry.lock change)
# ---------------------------------------------------------------------------
COPY pyproject.toml poetry.lock* ./
RUN poetry install --no-root --only main

# ---------------------------------------------------------------------------
# Application code
# ---------------------------------------------------------------------------
COPY . .

# dbt profiles directory (can be overridden at runtime)
ENV DBT_PROFILES_DIR=/app/dbt

# Ensure the virtual env is on PATH
ENV PATH="/app/.venv/bin:$PATH"

# ---------------------------------------------------------------------------
# Default command: run dbt deps then full build
# Override in docker-compose for specific tasks
# ---------------------------------------------------------------------------
CMD ["sh", "-c", "cd dbt && dbt deps && dbt run && dbt test"]
