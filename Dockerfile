FROM python:3.12-slim

WORKDIR /app

# PostgreSQL client library for Python.
RUN pip install --no-cache-dir psycopg2-binary

# ETL code + datasets.
COPY etl_pipeline.py ./etl_pipeline.py
COPY occupazione.csv ./occupazione.csv
COPY disoccupazione.csv ./disoccupazione.csv

CMD ["python", "etl_pipeline.py"]

