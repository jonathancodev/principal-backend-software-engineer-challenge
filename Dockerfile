FROM python:3.11-slim

WORKDIR /srv/app

# Install dependencies first for layer caching.
COPY pyproject.toml README.md ./
RUN pip install --no-cache-dir . && pip install --no-cache-dir ".[dev]"

COPY app ./app
COPY tests ./tests

# Reinstall the package itself now that sources are present (deps cached above).
RUN pip install --no-cache-dir --no-deps --force-reinstall .

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
