FROM python:3.13-slim
WORKDIR /app
COPY pyproject.toml poetry.lock ./
RUN pip install --no-cache-dir poetry && poetry config virtualenvs.create false && poetry install --only main --no-root
RUN pip install --no-cache-dir dbt-duckdb dbt-bigquery
COPY skyfeed skyfeed
COPY orchestrate/submitters/dbt_builder/main.py .
CMD ["python", "main.py"]
