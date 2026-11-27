import os
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk import DAG, task

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
ALERT_EMAIL = os.getenv("PIPELINE_ALERT_EMAIL")
SCHEDULE = os.getenv("PIPELINE_SCHEDULE") or None
TIMEZONE = os.getenv("PIPELINE_TIMEZONE", "UTC")
START_DATE = os.getenv("PIPELINE_START_DATE", "2026-01-01")


def failure_callback():
    if not ALERT_EMAIL:
        return None

    from airflow.providers.smtp.notifications.smtp import send_smtp_notification

    return send_smtp_notification(
        smtp_conn_id="smtp_default",
        to=ALERT_EMAIL,
        subject="Airflow DAG {{ dag.dag_id }} failed",
        html_content="Run {{ run_id }} failed. See the Airflow task logs for details.",
    )


@task(
    retries=int(os.getenv("PIPELINE_RETRIES", "2" if os.getenv("PIPELINE_SOURCE_IDS") else "0")),
    retry_delay=timedelta(minutes=5),
    retry_exponential_backoff=True,
    execution_timeout=timedelta(minutes=int(os.getenv("PIPELINE_TIMEOUT_MINUTES", "60"))),
)
def run_pipeline():
    sys.path.insert(0, str(SRC_DIR))
    try:
        from main import run

        try:
            run(raise_on_failure=True)
        except ValueError as error:
            if not os.getenv("PIPELINE_SOURCE_IDS"):
                raise
            raise AirflowFailException(str(error)) from None
    finally:
        sys.path.remove(str(SRC_DIR))


with DAG(
    dag_id="gdrive_to_postgres",
    description="Download Google Drive files and load transformed CSVs into PostgreSQL.",
    schedule=SCHEDULE,
    start_date=pendulum.parse(START_DATE, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    on_failure_callback=failure_callback(),
) as dag:
    run_pipeline()
