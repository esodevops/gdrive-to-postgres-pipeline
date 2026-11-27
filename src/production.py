"""Publish the three finance snapshots together, with archived inputs for replay."""

import hashlib
import io
import json
import logging
import os
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from config import DB_URL, DRIVE_FOLDER_ID
from extract import GSHEET_MIME
from extract import get_service, list_files
from transform import normalize_columns

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Dataset:
    key: str
    columns: str
    date: str
    amount: str | None = None
    parent: str | None = None


DATASETS = {
    "customers": Dataset(
        "customer_id",
        "customer_id full_name email_address phone_number city join_date customer_segment",
        "join_date",
    ),
    "accounts": Dataset(
        "account_id",
        "account_id customer_id account_type currency opening_balance open_date account_status",
        "open_date",
        "opening_balance",
        "customers",
    ),
    "transactions": Dataset(
        "transaction_id",
        "transaction_id account_id transaction_date transaction_type description category amount currency channel status",
        "transaction_date",
        "amount",
        "accounts",
    ),
}


def _money(value):
    try:
        amount = Decimal(value)
        if (
            amount.is_finite()
            and abs(amount) < Decimal("1e16")
            and amount == amount.quantize(Decimal("0.01"))
        ):
            return amount
    except InvalidOperation:
        pass
    return None


def validate_snapshot(name, payload, source_file):
    dataset = DATASETS[name]
    try:
        frame = pd.read_csv(io.BytesIO(payload), dtype=str, keep_default_na=False)
    except (pd.errors.ParserError, pd.errors.EmptyDataError, UnicodeError):
        raise ValueError(f"{name}: expected a well-formed UTF-8 CSV") from None
    if not isinstance(frame.index, pd.RangeIndex):
        raise ValueError(f"{name}: CSV row contains more fields than the header")
    frame.columns = normalize_columns(frame.columns)
    columns = dataset.columns.split()
    if sorted(frame.columns) != sorted(columns):
        raise ValueError(f"{name}: expected columns {', '.join(columns)}")

    frame = frame[columns].apply(lambda column: column.str.strip())
    key = frame[dataset.key]
    dates = pd.to_datetime(frame[dataset.date], format="%Y-%m-%d", errors="coerce")
    checks = {
        "missing key": key.eq(""),
        "duplicate key": key.ne("") & key.duplicated(keep=False),
        "invalid date": dates.isna(),
    }
    frame[dataset.date] = dates.dt.date

    if dataset.amount:
        amounts = frame[dataset.amount].map(_money)
        checks["invalid amount"] = amounts.isna()
        checks["invalid currency"] = ~frame["currency"].str.fullmatch(r"[A-Z]{3}")
        frame[dataset.amount] = amounts

    if name == "customers":
        frame["full_name"] = frame["full_name"].str.title()
        frame["email_address"] = frame["email_address"].str.lower()
    elif name == "accounts":
        frame["account_status"] = frame["account_status"].str.title()
    else:
        kind = frame["transaction_type"].str.title()
        income = kind.isin(["Deposit", "Refund"])
        outflow = kind.isin(["Purchase", "Withdrawal", "Transfer", "Fee"])
        checks["unknown transaction type"] = ~(income | outflow)
        frame["transaction_type"] = kind
        frame["description"] = frame["description"].str.title()
        frame["channel"] = frame["channel"].str.title()

    if frame.empty:
        raise ValueError(f"{name}: empty snapshots are rejected")
    failures = pd.DataFrame(checks)
    invalid_count = int(failures.any(axis=1).sum())
    if invalid_count:
        reasons = ", ".join(f"{reason}: {count}" for reason, count in failures.sum().items() if count)
        raise ValueError(f"{name}: {invalid_count} invalid rows out of {len(frame)} ({reasons})")

    if name == "transactions":
        frame["amount"] = amounts.where(~income, amounts.abs())
        frame["signed_amount"] = frame["amount"].where(~outflow, -amounts.abs())
    frame["source_file"] = source_file
    return frame


def capture_snapshot(s3, bucket, prefix):
    source_ids = json.loads(os.environ["PIPELINE_SOURCE_IDS"])
    if (
        not isinstance(source_ids, dict)
        or source_ids.keys() != DATASETS.keys()
        or any(not isinstance(value, str) or not value.strip() for value in source_ids.values())
        or len(set(source_ids.values())) != len(DATASETS)
    ):
        raise ValueError("PIPELINE_SOURCE_IDS must map each dataset to a distinct Drive file ID")
    if not DRIVE_FOLDER_ID:
        raise ValueError("DRIVE_FOLDER_ID is required")

    service = get_service()
    files = {file["id"]: file for file in list_files(service, DRIVE_FOLDER_ID)}
    manifest = {}
    for name, file_id in source_ids.items():
        if file_id not in files:
            raise ValueError(f"{name}: configured file is missing from the source folder")
        file = files[file_id]
        if file["mimeType"] == GSHEET_MIME:
            request = service.files().export_media(fileId=file_id, mimeType="text/csv")
        elif file["name"].lower().endswith(".csv"):
            request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
        else:
            raise ValueError(f"{name}: expected a CSV or Google Sheet")
        payload = request.execute(num_retries=2)
        digest = hashlib.sha256(payload).hexdigest()
        key = f"{prefix}/files/{file_id}/{digest}.csv"
        s3.put_object(Bucket=bucket, Key=key, Body=payload, ContentType="text/csv")
        manifest[name] = {"file_id": file_id, "sha256": digest, "key": key, "name": file["name"]}

    identity = {name: [entry["file_id"], entry["sha256"]] for name, entry in manifest.items()}
    batch_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    s3.put_object(
        Bucket=bucket, Key=f"{prefix}/batches/{batch_id}.json",
        Body=json.dumps(manifest).encode(), ContentType="application/json",
    )
    return batch_id


def publish_snapshot(engine, batch_id, frames, manifest):
    counts = {name: len(frame) for name, frame in frames.items()}
    with engine.begin() as conn:
        conn.execute(text("SELECT pg_advisory_xact_lock(748239101)"))
        published = conn.execute(
            text("SELECT 1 FROM ingestion.published_batches WHERE batch_id = :batch_id"),
            {"batch_id": batch_id},
        ).scalar()
        if published:
            log.info("Batch %s was already published", batch_id)
            return

        published_at = conn.execute(text("SELECT clock_timestamp()")).scalar_one()
        for name in reversed(DATASETS):
            conn.execute(text(f"DELETE FROM staging.{name}"))
        for name in DATASETS:
            frames[name].assign(loaded_at=published_at).to_sql(
                name, conn, schema="staging", if_exists="append", index=False,
                method="multi", chunksize=1000,
            )
        conn.execute(
            text("""
                INSERT INTO ingestion.published_batches (batch_id, published_at, sources, row_counts)
                VALUES (:batch_id, :published_at, CAST(:sources AS jsonb), CAST(:counts AS jsonb))
            """),
            {"batch_id": batch_id, "published_at": published_at,
             "sources": json.dumps(manifest), "counts": json.dumps(counts)},
        )
    log.info("Published batch %s with row counts %s", batch_id, counts)


def _read_object(s3, bucket, key):
    with s3.get_object(Bucket=bucket, Key=key)["Body"] as body:
        return body.read()


def run_snapshot(batch_id=None):
    import boto3

    bucket = os.environ["PIPELINE_ARCHIVE_BUCKET"]
    prefix = os.getenv("PIPELINE_ARCHIVE_PREFIX", "finance").strip("/")
    s3 = boto3.client("s3")
    if batch_id is None:
        batch_id = capture_snapshot(s3, bucket, prefix)
    elif not re.fullmatch(r"[a-f0-9]{64}", batch_id):
        raise ValueError("Replay requires a 64-character batch ID from the pipeline logs")
    log.info("Validating batch %s", batch_id)
    manifest = json.loads(_read_object(s3, bucket, f"{prefix}/batches/{batch_id}.json"))

    frames = {}
    for name, dataset in DATASETS.items():
        entry = manifest[name]
        payload = _read_object(s3, bucket, entry["key"])
        if hashlib.sha256(payload).hexdigest() != entry["sha256"]:
            raise ValueError(f"{name}: archived content failed checksum verification")
        frame = validate_snapshot(name, payload, entry["name"])
        if dataset.parent:
            foreign_key = DATASETS[dataset.parent].key
            orphan_count = (~frame[foreign_key].isin(frames[dataset.parent][foreign_key])).sum()
            if orphan_count:
                raise ValueError(f"{name}: {orphan_count} rows lack a matching {dataset.parent} record")
        frames[name] = frame

    engine = create_engine(DB_URL, hide_parameters=True)
    try:
        publish_snapshot(engine, batch_id, frames, manifest)
    except (pd.errors.DatabaseError, SQLAlchemyError) as error:
        cause = error.__cause__ if isinstance(error, pd.errors.DatabaseError) else error
        code = getattr(getattr(cause, "orig", None), "pgcode", None)
        log.error("Database publish failed for batch %s (SQLSTATE %s)", batch_id, code or "unavailable")
        raise RuntimeError(f"Database publish failed for batch {batch_id}") from None
    finally:
        engine.dispose()


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay", metavar="BATCH_ID")
    run_snapshot(parser.parse_args().replay)
