FROM python:3.12.8-slim-bookworm

WORKDIR /app

COPY requirements-ingestion.txt /app/requirements-ingestion.txt
RUN python -m pip install --no-cache-dir --requirement /app/requirements-ingestion.txt

COPY src /app/src
COPY scripts /app/scripts
COPY tests /app/tests

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app/src:/app

ENTRYPOINT ["python", "-m", "bronze_ingestion.cli"]
