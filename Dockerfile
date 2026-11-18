FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
COPY dags/ dags/

RUN useradd --create-home pipeline \
    && install -d -o pipeline -g pipeline /app/data/raw /home/pipeline/airflow

USER pipeline
CMD ["airflow", "standalone"]
