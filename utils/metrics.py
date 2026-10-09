
import json
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
METRICS_DIR = PROJECT_ROOT / "pipeline_metrics"
METRICS_FILE = METRICS_DIR / "latest_run.json"
PIPELINE_STATUS_FILE = METRICS_DIR / "pipeline_run.json"


def _write_json(file_path: Path, data: dict) -> None:
    METRICS_DIR.mkdir(parents=True, exist_ok=True)

    with file_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)


def record_metrics(
    total_records: int,
    valid_records: int,
    invalid_records: int,
    duplicate_shipments: int,
    status: str,
    duration_seconds: float,
) -> None:
    """Save actual data-quality validation metrics."""

    metrics = {
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "total_records": total_records,
        "valid_records": valid_records,
        "invalid_records": invalid_records,
        "duplicate_shipments": duplicate_shipments,
        "status": status,
        "duration_seconds": round(duration_seconds, 2),
    }

    _write_json(METRICS_FILE, metrics)
    print(f"\nValidation metrics saved to: {METRICS_FILE}")
    print(json.dumps(metrics, indent=4))


def record_pipeline_status(
    status: str,
    duration_seconds: float,
    failed_step: str | None = None,
) -> None:
    """Save the status and duration of the entire pipeline run."""

    result = {
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "duration_seconds": round(duration_seconds, 2),
        "failed_step": failed_step,
    }

    _write_json(PIPELINE_STATUS_FILE, result)
    print(f"\nPipeline status saved to: {PIPELINE_STATUS_FILE}")
    print(json.dumps(result, indent=4))