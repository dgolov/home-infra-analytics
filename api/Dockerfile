########################
# Stage 1: builder
########################
FROM python:3.12-slim AS builder

WORKDIR /api

ENV POETRY_VERSION=2.3.1
ENV POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1

RUN pip install --no-cache-dir "poetry==$POETRY_VERSION"

COPY pyproject.toml poetry.lock* ./

RUN poetry install --no-root --only main

COPY . .


########################
# Stage 2: runtime
########################
FROM python:3.12-slim

WORKDIR /api

RUN apt-get update \
    && apt-get install -y --no-install-recommends iputils-ping \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local /usr/local
COPY --from=builder /api /api

RUN chmod +x /api/run.sh

EXPOSE 8000

CMD ["/api/run.sh"]
