FROM ghcr.io/astral-sh/uv:0.13.7-trixie-slim@sha256:f6da342fe598dc462a7e6f61e7b3bf7fc84e381467efbdf29ccc1d32f2fc68a4 AS builder

WORKDIR /app
COPY pyproject.toml uv.lock /app/

RUN uv python install 3.13 
RUN uv export --locked --no-dev --no-emit-project -o requirements.txt
RUN uv pip install --system --target=/app/dependencies --only-binary :all: -r requirements.txt

# Create application user 
RUN groupadd --gid 180291 microdata \
    && useradd --uid 180291 --gid microdata microdata

# Production image
FROM gcr.io/distroless/python3-debian13:nonroot@sha256:c3ed2b61f3c4e410685565ceb3c818197b552832521b39d53eca5d2d9d0dc1e1
ARG COMMIT_ID
ENV COMMIT_ID=$COMMIT_ID

WORKDIR /app
COPY datastore_api datastore_api
COPY migrations migrations

COPY --from=builder /app/dependencies /app/dependencies
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group

USER microdata
ENV PYTHONPATH "${PYTHONPATH}:/app:/app/dependencies"
CMD ["-m", "gunicorn", "--no-control-socket", "-k", "uvicorn.workers.UvicornWorker", "--logger-class", "datastore_api.config.logging.gunicorn.CustomLogger", "datastore_api.main:app", "--preload", "--workers", "2", "--bind", "0.0.0.0:8000"] 
