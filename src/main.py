import logging
import os
import pandas as pd
from config import DRIVE_FOLDER_ID
from extract import get_service, list_files, download_file
from transform import transform
from load import load

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pipeline")


def run(*, raise_on_failure: bool = False):
    if os.getenv("PIPELINE_SOURCE_IDS"):
        from production import run_snapshot

        run_snapshot()
        return

    service = get_service()
    files = list_files(service, DRIVE_FOLDER_ID)
    log.info("Found %d file(s) in Drive folder", len(files))

    failed_count = 0
    for f in files:
        try:
            path = download_file(service, f)
            if path.suffix.lower() != ".csv":
                log.warning("Skipping non-CSV file: %s", path.name)
                continue

            raw = pd.read_csv(path)
            clean = transform(raw, source_file=path.name)
            table = path.stem.lower().replace(" ", "_")
            rows = load(clean, table, source_file=path.name)
            log.info(
                "%s: %d raw rows -> %d loaded into staging.%s",
                path.name,
                len(raw),
                rows,
                table,
            )
        except Exception:
            failed_count += 1
            log.exception("Failed processing %s", f["name"])

    if raise_on_failure and failed_count:
        raise RuntimeError(f"Failed to process {failed_count} Drive file(s). See task logs.")


if __name__ == "__main__":
    run()
