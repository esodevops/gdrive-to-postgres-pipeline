-- Apply once to a separate, empty production database as the migration owner.
BEGIN;
CREATE SCHEMA staging;
CREATE SCHEMA ingestion;
CREATE SCHEMA reporting;

CREATE TABLE staging.customers (
    customer_id text PRIMARY KEY,
    full_name text,
    email_address text,
    phone_number text,
    city text,
    join_date date NOT NULL,
    customer_segment text,
    source_file text NOT NULL,
    loaded_at timestamptz NOT NULL
);

CREATE TABLE staging.accounts (
    account_id text PRIMARY KEY,
    customer_id text NOT NULL REFERENCES staging.customers,
    account_type text,
    currency text NOT NULL,
    opening_balance numeric(18, 2) NOT NULL,
    open_date date NOT NULL,
    account_status text,
    source_file text NOT NULL,
    loaded_at timestamptz NOT NULL
);

CREATE TABLE staging.transactions (
    transaction_id text PRIMARY KEY,
    account_id text NOT NULL REFERENCES staging.accounts,
    transaction_date date NOT NULL,
    transaction_type text NOT NULL,
    description text,
    category text,
    amount numeric(18, 2) NOT NULL,
    currency text NOT NULL,
    channel text,
    status text,
    signed_amount numeric(18, 2) NOT NULL,
    source_file text NOT NULL,
    loaded_at timestamptz NOT NULL
);

CREATE TABLE ingestion.published_batches (
    batch_id text PRIMARY KEY,
    published_at timestamptz NOT NULL,
    sources jsonb NOT NULL,
    row_counts jsonb NOT NULL
);

CREATE VIEW reporting.customers AS
    SELECT customer_id, city, join_date, customer_segment FROM staging.customers;
CREATE VIEW reporting.accounts AS
    SELECT account_id, customer_id, account_type, currency, opening_balance,
           open_date, account_status FROM staging.accounts;
CREATE VIEW reporting.transactions AS
    SELECT transaction_id, account_id, transaction_date, transaction_type,
           category, amount, currency, channel, status, signed_amount
    FROM staging.transactions;
CREATE VIEW reporting.refresh_status AS
    SELECT batch_id, published_at, row_counts
    FROM ingestion.published_batches
    ORDER BY published_at DESC
    LIMIT 1;
COMMIT;
