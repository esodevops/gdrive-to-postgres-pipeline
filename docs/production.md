# Deploy the finance snapshot pipeline

Production mode uses an organization-managed Google Drive folder, Airflow, private S3 storage and PostgreSQL. It publishes the three datasets as one full snapshot. Local Compose remains the development stack. Run the production DAG on an existing Airflow installation and install the pipeline dependencies on its workers. Keep the DAG and `src/` in the same relative layout as this repository.

## Establish the delivery contract

- Keep one stable Drive file ID per dataset. For a CSV, upload a new version of the same file. For a Google Sheet, update the first tab in place. Use the column headers defined in [production.py](../src/production.py), ISO dates, three uppercase currency letters, and amounts with at most two decimal places.
- Deliver complete snapshots after the upstream refresh finishes and before the agreed cutoff. Each business key must appear once, and every account and transaction must reference a parent in that delivery. Empty datasets, unknown transaction types, and any invalid row block publication. The local demo data can require cleanup before it passes these rules.
- With a cron schedule, the producer must finish replacing all three inputs before the DAG starts. If the files can arrive late or independently, have the producer trigger the DAG after the full delivery is ready. The pipeline validates consistency between datasets, but Drive content alone cannot identify the accounting period. If Finance supplies reconciliation totals, check them against the published counts and reporting queries during rollout.

## Provision and configure

1. Provision private PostgreSQL with backups and tested restores. Use a new database for this migration because existing demo tables use inferred schemas. Run `psql -X -v ON_ERROR_STOP=1 -f sql/001_production.sql` as the migration owner. Use TLS and grant separate access to the loader and reporting accounts. For existing roles named `finance_loader` and `finance_bi`, apply these grants as the migration owner.

   ```sql
   GRANT USAGE ON SCHEMA staging, ingestion TO finance_loader;
   GRANT SELECT, INSERT, DELETE ON ALL TABLES IN SCHEMA staging TO finance_loader;
   GRANT SELECT, INSERT ON ingestion.published_batches TO finance_loader;

   GRANT USAGE ON SCHEMA reporting TO finance_bi;
   GRANT SELECT ON ALL TABLES IN SCHEMA reporting TO finance_bi;
   ```

2. Create a private, encrypted S3 bucket with a lifecycle policy and give the worker role only object read/write access under the pipeline prefix. The archive contains customer data. Set its retention to match the agreed replay period.
3. Enable the Drive API and share the source folder with the Google identity used by the worker. Use Application Default Credentials where the worker identity supports Drive read access. For a key-based service account, inject the key from a secrets manager and set `SERVICE_ACCOUNT_FILE` to its mounted path.
4. Set `PIPELINE_SOURCE_IDS`, `DRIVE_FOLDER_ID`, `PIPELINE_ARCHIVE_BUCKET`, and the database environment variables on the workers. Set the schedule and retry variables on the DAG processor as well, including `PIPELINE_SOURCE_IDS` so the DAG selects production retry defaults. Configure `smtp_default` and `PIPELINE_ALERT_EMAIL` for failure alerts. Trigger a smoke run in the deployed Airflow environment before enabling cron.
5. Host Metabase with PostgreSQL as its application database and add the finance database with `finance_bi`. Point dashboards at `reporting` and display `reporting.refresh_status.published_at`. Keep Metabase application data and reporting data in separate databases. Use the team's monitor to alert when the last published batch is older than the agreed reporting deadline. An unchanged delivery exits successfully without advancing the published timestamp.

## Verify and recover

- Run alongside the existing finance report for at least one full reporting cycle. Compare row counts, published timestamps, and transaction totals by currency and type. Exercise a wrong schema, duplicate key, missing file and a database error before switching dashboards.
- For a failed run, check the task log for the batch ID or the missing dataset. Finance can correct the source and trigger the DAG again. The previous published data remains available while checks run.
- To retry an archived batch that has never published, pause the DAG, wait for active runs to finish, and run `python src/production.py --replay BATCH_ID` with the production worker's credentials and environment. Verify that the archived delivery should become the current report before replaying it. Previously published batch IDs are skipped. Use the audit table to verify the resulting batch and row counts.
- Build releases through the CI workflow, promote the same reviewed image or source revision to staging and production, and apply database migrations before deploying code that depends on them. The workflow checks the pipeline and builds the image. Registry and hosted Airflow deployment credentials are environment-specific.
