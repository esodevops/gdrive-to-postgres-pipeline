# Google Drive to PostgreSQL Pipeline

This batch pipeline reads CSV files and Google Sheets from a Google Drive folder, cleans the data with pandas, and loads it into PostgreSQL's `staging` schema. Run it through Airflow to track task status and logs, or execute it directly for a one-off load.

```text
Google Drive folder → CSV download or export → pandas cleanup → PostgreSQL staging tables
                          Airflow runs and monitors the pipeline
```

## Prerequisites

- Docker with Docker Compose for the containerized setup.
- A Google Cloud project with the Google Drive API enabled, a service account, and a JSON key for that account.
- A Google Drive folder shared with the service account's email address. Viewer access is sufficient when downloads and exports are permitted.
- Python 3.13 for the optional local setup. The Docker image already includes Python and the project dependencies.

## Configure the pipeline

1. Copy the environment template from the repository root.

   ```bash
   cp .env.example .env
   ```

2. Save the service account key as `credentials/credentials_service_account.json`. The example JSON file documents the expected format. Keep the real key and `.env` out of version control.
3. Open the source folder in Google Drive and copy the ID from its `/folders/<folder-id>` URL. Set `DRIVE_FOLDER_ID` in `.env`.
4. Set `DB_USER` and `DB_PASSWORD` in `.env`. Compose uses these values and `DB_NAME` when initializing a new PostgreSQL volume.

| Variable | Purpose |
| --- | --- |
| `DRIVE_FOLDER_ID` | ID of the folder containing the input files. |
| `SERVICE_ACCOUNT_FILE` | Path to the service account JSON key. The template points to `credentials/credentials_service_account.json`. Compose mounts it as a read-only secret. |
| `DB_NAME` | Database to load into. The Compose default is `finance_db`. |
| `DB_USER`, `DB_PASSWORD` | PostgreSQL credentials. |
| `DB_HOST`, `DB_PORT` | Connection settings for a local Python run. Use `localhost:5434` to reach the Compose database from your computer. Compose services use `postgres:5432` internally. |
| `AIRFLOW_PORT` | Optional host port for the Airflow UI. Defaults to `8080`. |
| `PIPELINE_ALERT_EMAIL` | Optional recipient for failed DAG runs. Configure an SMTP connection named `smtp_default` in Airflow before enabling alerts. |

## Run with Airflow and Docker Compose

From the repository root, start PostgreSQL and Airflow.

```bash
docker compose up --build -d
docker compose ps
```

Once Airflow has started, retrieve the generated local admin password.

```bash
docker compose exec airflow cat /home/pipeline/airflow/simple_auth_manager_passwords.json.generated
```

1. Open the Airflow UI at `http://localhost:8080`, or use your configured `AIRFLOW_PORT`.
2. Sign in as `admin` with the generated password.
3. Find `gdrive_to_postgres`, unpause it if needed, and trigger a run.
4. Open the `run_pipeline` task logs to see the source files, row counts, and errors.

The DAG has no automatic schedule. It allows one active run at a time and has zero task retries. If any file fails, the task reports failure after attempting the remaining files. Airflow Standalone is intended for local development.

## Run without Airflow

Run the one-shot Compose service. It starts PostgreSQL if needed, waits for it to become healthy, and exits with an error if any file fails.

```bash
docker compose run --build --rm pipeline
```

To run using local Python, point `DB_HOST` and `DB_PORT` at an existing PostgreSQL instance. The database must exist, and the configured role needs permission to create and write to the `staging` schema. For the bundled database, start it with `docker compose up -d postgres` and set `DB_PORT=5434` in `.env`.

```bash
python3.13 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python src/main.py
```

The local script logs individual file failures and continues. Check the logs for errors even when the process exits successfully.

## Input and output

The pipeline lists direct, non-trashed entries in the Drive folder. It exports Google Sheets as CSV and processes downloaded files with a `.csv` extension. Downloads are written to `data/raw/` in the environment running the pipeline. That directory is inside the container for Compose runs.

All CSV files have their column names normalized, fully empty rows and duplicate rows removed, and `source_file` and UTC `loaded_at` columns added. The three filenames below also receive dataset-specific transformations.

| Input file stem | Expected columns after normalization | Additional transformations |
| --- | --- | --- |
| `customers` | `full_name`, `email_address`, `join_date` | Title-cases names, lowercases emails, and parses join dates. |
| `accounts` | `account_status`, `opening_balance`, `open_date` | Title-cases statuses and parses balances and dates. |
| `transactions` | `transaction_date`, `amount`, `description`, `channel`, `transaction_type` | Parses dates and amounts, drops rows missing either value, and derives `signed_amount` from the transaction type. |

Each file maps to a table in `staging` using the lowercase filename stem with spaces replaced by underscores. Use simple, distinct stems such as `customers` or `monthly_accounts`. When reloading a file with the same name, the loader deletes its previous rows before appending the new batch. Those operations run in separate transactions, so an insert failure can leave that file's previous rows removed until a successful rerun.

## Verify the load

Open `psql` inside the PostgreSQL container using its configured database and role.

```bash
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

List the tables and inspect a sample after loading `customers.csv`.

```sql
SELECT tablename
FROM pg_tables
WHERE schemaname = 'staging'
ORDER BY tablename;

SELECT source_file, COUNT(*) AS row_count, MAX(loaded_at) AS latest_load
FROM staging.customers
GROUP BY source_file;
```

To inspect the database in pgAdmin on your computer, connect to host `localhost` on port `5434` with the database credentials from `.env`. See the [example pgAdmin result](docs/pgAdmin_result.png).

