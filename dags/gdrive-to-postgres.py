import os
import sys
from pathlib import Path

from airflow.sdk import DAG, task

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
ALERT_EMAIL = os.getenv("PIPELINE_ALERT_EMAIL")


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


@task(retries=0)
def run_pipeline():
    sys.path.insert(0, str(SRC_DIR))
    try:
        from main import run

        run(raise_on_failure=True)
    finally:
        sys.path.remove(str(SRC_DIR))


with DAG(
    dag_id="gdrive_to_postgres",
    description="Download Google Drive files and load transformed CSVs into PostgreSQL.",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    on_failure_callback=failure_callback(),
) as dag:
    run_pipeline()
