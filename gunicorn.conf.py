"""gunicorn settings shared by the Docker image (Railway) and a plain systemd install on a Linux server.

Everything can be overridden with environment variables. gthread workers keep long uploads and downloads from
being killed by the 30 second default of the sync worker; result files are uploaded in small chunks anyway.
"""
import os

# Behind nginx on a server set GUNICORN_BIND=127.0.0.1:8000 so only the proxy can reach the app.
bind = os.getenv("GUNICORN_BIND", "0.0.0.0:8000")
workers = int(os.getenv("WEB_CONCURRENCY", "2"))
worker_class = "gthread"
threads = int(os.getenv("GUNICORN_THREADS", "4"))
timeout = int(os.getenv("GUNICORN_TIMEOUT", "180"))
graceful_timeout = 30

# gunicorn 26 opens a control socket under the user's home directory by default. The service does not need it,
# and it should not depend on a writable home (systemd hardening, containers). Older versions ignore this name.
control_socket_disable = True
