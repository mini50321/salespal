FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app /app/app

ENV PORT=8080

CMD ["gunicorn", "-b", "0.0.0.0:8080", "app.main:app"]
