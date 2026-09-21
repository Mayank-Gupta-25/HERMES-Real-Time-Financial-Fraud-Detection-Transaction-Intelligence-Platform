FROM python:3.11-slim

WORKDIR /app

RUN pip install poetry==1.8.2 && \
    poetry config virtualenvs.create false

COPY pyproject.toml poetry.lock* ./
# Fallback to normal install if lock file is missing (to prevent error)
RUN poetry install --no-dev --no-interaction || poetry install --no-dev

COPY app/ ./app/

EXPOSE 8000
CMD ["tail", "-f", "/dev/null"]
