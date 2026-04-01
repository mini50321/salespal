FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Needed for stitching multiple short clips into a longer MP4.
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*

COPY app /app/app

ENV PORT=8080

# Long timeout: Vertex Veo video generation polls for many minutes; Cloud Run + gunicorn must stay alive.
CMD exec gunicorn --bind 0.0.0.0:${PORT} --workers 1 --threads 8 --timeout 3600 --graceful-timeout 30 app.main:app
