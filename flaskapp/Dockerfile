FROM python:3.12-slim

RUN pip install --no-cache-dir flask redis huey psutil humanize

EXPOSE 5000
ENV FLASK_APP=app.py

WORKDIR /app

# App code
COPY flaskapp/app/ /app

# Copy the single source of truth for tasks from the worker dir
COPY worker/app/tasks.py /app/tasks.py

CMD ["python", "-m", "flask", "run", "--host=0.0.0.0"]