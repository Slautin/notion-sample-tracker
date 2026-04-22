FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY pyproject.toml README.md ./
COPY notion_sample_tracker ./notion_sample_tracker

RUN pip install --no-cache-dir .

EXPOSE 8000
CMD ["gunicorn", "notion_sample_tracker:create_app()", "--bind", "0.0.0.0:8000", "--workers", "2"]
