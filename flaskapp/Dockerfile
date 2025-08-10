FROM python:3.12-slim

RUN pip install --no-cache-dir flask redis huey psutil humanize

EXPOSE 5000
ENV FLASK_APP=app.py

WORKDIR /app
COPY app/ /app


CMD ["python", "-m", "flask", "run", "--host=0.0.0.0"]