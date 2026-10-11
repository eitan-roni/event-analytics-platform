# One image for the API (default command), the consumer and migrations;
# docker-compose.yml picks the command per service.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first, so this layer is reused until requirements.txt changes.
# Runtime dependencies only; test tools live in requirements-dev.txt.
#
# Behind a TLS-intercepting proxy or antivirus, pass a CA bundle that trusts
# it as a build secret (it is not stored in the image):
#   docker build --secret id=pip_ca,src=path/to/ca-bundle.pem .
COPY requirements.txt .
RUN --mount=type=secret,id=pip_ca,required=false \
    if [ -f /run/secrets/pip_ca ]; then export PIP_CERT=/run/secrets/pip_ca; fi \
    && pip install -r requirements.txt

RUN useradd --create-home --uid 10001 app

COPY alembic.ini .
COPY migrations ./migrations
COPY app ./app

USER app

EXPOSE 8000

# Exec form: the process is PID 1 and receives SIGTERM from `docker stop`.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
