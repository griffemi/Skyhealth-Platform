FROM python:3.13-slim
WORKDIR /app
COPY orchestrate/submitters/spark_submitter/main.py .
COPY skyfeed skyfeed
RUN pip install --no-cache-dir requests pydantic
CMD ["python", "main.py"]
