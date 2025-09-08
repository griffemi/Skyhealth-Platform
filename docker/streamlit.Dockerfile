FROM python:3.13-slim
WORKDIR /app
COPY pyproject.toml poetry.lock ./
RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false && \
    poetry install --only main --no-root
COPY . .
CMD ["streamlit", "run", "apps/streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
