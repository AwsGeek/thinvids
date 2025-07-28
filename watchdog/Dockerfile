FROM python:3.12-slim

WORKDIR /app
COPY app/ /app

RUN pip install --no-cache-dir requests watchdog redis

CMD ["python", "watcher.py"]