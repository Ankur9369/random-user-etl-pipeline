"""Extract, transform, validate, and store Random User data."""

from dataclasses import dataclass
from datetime import datetime
import json
import logging
from pathlib import Path
import sqlite3
import time
from typing import Any

import pandas as pd
import requests


@dataclass(frozen=True)
class Settings:
    api_url: str = "https://randomuser.me/api/"
    batch_size: int = 1000
    total_batches: int = 5
    timeout: int = 30
    retry_attempts: int = 3
    retry_delay: int = 2
    output_dir: Path = Path("output")
    database_path: Path = Path("etl_database.db")
    backup_dir: Path = Path("backups")
    reports_dir: Path = Path("reports")


SETTINGS = Settings()
LOGGER = logging.getLogger(__name__)
KEEP_COLUMNS = {
    "gender": "gender",
    "name_first": "first_name",
    "name_last": "last_name",
    "email": "email",
    "phone": "phone",
    "dob_age": "age",
    "location_country": "country",
    "location_city": "city",
    "location_state": "state",
    "login_username": "username",
    "picture_large": "photo_url",
}
REQUIRED_COLUMNS = {"email", "first_name", "last_name", "age", "country"}

# config and logging setup
def configure_logging() -> None:
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"etl_{datetime.now():%Y%m%d_%H%M%S}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )

# extract functions
def extract_batch(batch_number: int) -> list[dict[str, Any]]:
    """Fetch one batch and retry transient request failures."""
    for attempt in range(SETTINGS.retry_attempts):
        try:
            LOGGER.info(
                "Extracting batch %s/%s (attempt %s)",
                batch_number + 1,
                SETTINGS.total_batches,
                attempt + 1,
            )
            response = requests.get(
                SETTINGS.api_url,
                params={"results": SETTINGS.batch_size},
                timeout=SETTINGS.timeout,
            )
            response.raise_for_status()
            users = response.json().get("results", [])
            LOGGER.info("Batch %s extracted: %s records", batch_number + 1, len(users))
            return users
        except requests.RequestException as error:
            LOGGER.warning("Batch %s failed: %s", batch_number + 1, error)
            if attempt + 1 < SETTINGS.retry_attempts:
                time.sleep(SETTINGS.retry_delay**attempt)

    LOGGER.error("Batch %s failed after all retries", batch_number + 1)
    return []


def extract_data() -> list[dict[str, Any]]:
    users: list[dict[str, Any]] = []
    for batch_number in range(SETTINGS.total_batches):
        users.extend(extract_batch(batch_number))
        if batch_number + 1 < SETTINGS.total_batches:
            time.sleep(1)
    LOGGER.info("Total extracted: %s users", len(users))
    return users

# transform, validate, and load functions are defined below
def transform_data(raw_data: list[dict[str, Any]]) -> pd.DataFrame:
    """Flatten API data, keep useful fields, and add derived values."""
    if not raw_data:
        return pd.DataFrame()

    frame = pd.json_normalize(raw_data, sep="_")
    available = [column for column in KEEP_COLUMNS if column in frame.columns]
    frame = frame[available].rename(columns={column: KEEP_COLUMNS[column] for column in available})

    if "email" in frame:
        frame = frame.drop_duplicates(subset="email")
    frame = frame.fillna({column: "Unknown" for column in frame.columns if column != "age"})
    frame["age"] = pd.to_numeric(frame.get("age"), errors="coerce")
    frame["full_name"] = frame["first_name"] + " " + frame["last_name"]
    frame["age_group"] = pd.cut(
        frame["age"],
        bins=[0, 18, 25, 35, 50, 65, 100],
        labels=["<18", "18-24", "25-34", "35-49", "50-64", "65+"],
    )
    frame["extracted_at"] = datetime.now()
    frame["pipeline_version"] = "2.0"
    return frame
# validate and load functions

def validate_data(frame: pd.DataFrame) -> bool:
    """Run data-quality checks and return whether they all pass."""
    checks = {
        "data is not empty": not frame.empty,
        "required columns exist": REQUIRED_COLUMNS.issubset(frame.columns),
    }
    if "email" in frame:
        checks["emails are not null"] = frame["email"].notna().all()
        checks["emails are unique"] = not frame["email"].duplicated().any()
    if "age" in frame:
        checks["ages are between 18 and 100"] = frame["age"].between(18, 100).all()

    for name, passed in checks.items():
        LOGGER.info("%s: %s", "PASS" if passed else "FAIL", name)
    return all(checks.values())

#save and report functions
def save_file(frame: pd.DataFrame, path: Path, writer: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    writer(path)
    LOGGER.info("Saved %s", path)
    return path

#load function
def load_data(frame: pd.DataFrame) -> dict[str, Path]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    writers = {
        "csv": (SETTINGS.output_dir / f"users_{timestamp}.csv", lambda path: frame.to_csv(path, index=False)),
        "json": (SETTINGS.output_dir / f"users_{timestamp}.json", lambda path: frame.to_json(path, orient="records", indent=2)),
        "backup": (SETTINGS.backup_dir / f"users_backup_{timestamp}.csv", lambda path: frame.to_csv(path, index=False)),
    }
    saved: dict[str, Path] = {}
    for name, (path, writer) in writers.items():
        try:
            saved[name] = save_file(frame, path, writer)
        except (OSError, ValueError) as error:
            LOGGER.warning("Could not save %s: %s", name, error)

    try:
        with sqlite3.connect(SETTINGS.database_path) as connection:
            frame.to_sql("users", connection, if_exists="append", index=False)
        saved["database"] = SETTINGS.database_path
        LOGGER.info("Saved database %s", SETTINGS.database_path)
    except (OSError, sqlite3.Error, ValueError) as error:
        LOGGER.warning("Could not save database: %s", error)

    return saved

#report generation function
def generate_report(frame: pd.DataFrame, extracted: int, started: float) -> Path:
    elapsed = time.time() - started
    processed = len(frame)
    report = {
        "timestamp": datetime.now().isoformat(),
        "pipeline_version": "2.0",
        "records_extracted": extracted,
        "records_processed": processed,
        "total_time_seconds": elapsed,
        "records_per_second": processed / elapsed if elapsed else 0,
        "memory_mb": frame.memory_usage(deep=True).sum() / 1024**2,
        "countries": int(frame["country"].nunique()) if "country" in frame else 0,
        "avg_age": float(frame["age"].mean()) if "age" in frame else None,
        "status": "SUCCESS",
    }
    report_path = SETTINGS.reports_dir / f"report_{datetime.now():%Y%m%d_%H%M%S}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    LOGGER.info("Saved report %s", report_path)
    return report_path
#main pipeline function

def run_pipeline() -> bool:
    started = time.time()
    try:
        raw_data = extract_data()
        if not raw_data:
            LOGGER.error("No data extracted; pipeline stopped")
            return False

        frame = transform_data(raw_data)
        if frame.empty:
            LOGGER.error("Transformation produced no data; pipeline stopped")
            return False

        if not validate_data(frame):
            LOGGER.warning("Validation failed; continuing with load")
        load_data(frame)
        generate_report(frame, len(raw_data), started)
        LOGGER.info("ETL pipeline completed successfully")
        return True
    except Exception:
        LOGGER.exception("Pipeline failed")
        return False

# main entry point
if __name__ == "__main__":
    configure_logging()
    raise SystemExit(0 if run_pipeline() else 1)
