FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    openssh-client \
  && rm -rf /var/lib/apt/lists/*

COPY requirements-manager.txt /app/requirements-manager.txt
RUN python -m pip install --upgrade pip setuptools wheel && \
    python -m pip install -r /app/requirements-manager.txt

COPY common.py /app/common.py
COPY manager/ /app/
COPY worker/tasks.py /app/tasks.py

EXPOSE 5005

CMD ["gunicorn", "-w", "2", "-t", "120", "-b", "0.0.0.0:5005", "app:app"]
