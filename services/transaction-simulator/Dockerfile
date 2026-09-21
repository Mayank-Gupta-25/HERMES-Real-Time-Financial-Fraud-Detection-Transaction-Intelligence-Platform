FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY tests/ ./tests/

ENV PYTHONUNBUFFERED=1

# Defaults can be overridden at `docker run` time, e.g.:
#   docker run hermes/transaction-simulator --tps 10000 --duration 300
ENTRYPOINT ["python", "-m", "app.main"]
CMD ["--tps", "1000", "--duration", "60"]
