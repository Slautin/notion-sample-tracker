FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV BACKLOG_DIR=/data/backlog

WORKDIR /app
COPY pyproject.toml README.md ./
COPY notion_sample_tracker ./notion_sample_tracker

RUN pip install --no-cache-dir .
RUN useradd --create-home --shell /usr/sbin/nologin appuser \
    && mkdir -p /data/backlog \
    && chown -R appuser:appuser /data

EXPOSE 8000
VOLUME ["/data"]
USER appuser
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=3).read()"
CMD ["gunicorn", "notion_sample_tracker:create_app()", "--bind", "0.0.0.0:8000", "--workers", "2"]
