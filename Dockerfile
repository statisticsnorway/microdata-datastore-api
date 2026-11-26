FROM ghcr.io/astral-sh/uv:0.13.10-trixie-slim@sha256:8b48445a9a356d395bc87d0cc15d6a65e91e67424562f6de9fdcd6205ac3f040 AS builder

WORKDIR /app
COPY pyproject.toml uv.lock /app/

RUN uv python install 3.13 
RUN uv export --locked --no-dev --no-emit-project -o requirements.txt
RUN uv pip install --system --target=/app/dependencies --only-binary :all: -r requirements.txt

# Create application user 
RUN groupadd --gid 180291 microdata \
    && useradd --uid 180291 --gid microdata microdata

# Production image
FROM gcr.io/distroless/python3-debian13:nonroot@sha256:72de1dda9d4c52c370e2ef2d93c28e3e1a8193c92058b7dfa42bace38a83d114
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
