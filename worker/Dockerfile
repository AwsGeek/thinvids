FROM python:3.12-slim

RUN apt-get update && apt-get install -y ffmpeg && apt-get clean && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir requests redis celery

WORKDIR /app
COPY app/ /app
