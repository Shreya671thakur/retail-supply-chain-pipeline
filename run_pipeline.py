
import subprocess
import sys
import time
from pathlib import Path

from utils.metrics import record_pipeline_status


PROJECT_ROOT = Path(__file__).resolve().parent

STEPS = [
    (
        "ETL Transformation",
        [
            sys.executable,
            "pyspark/transform.py",
            "--input-dir",
            "datasets",
            "--output-dir",
            "output",
        ],
    ),
    (
        "Data Quality Validation",
        [
            sys.executable,
            "-m",
            "data_quality.validate_data",
        ],
    ),
    (
        "SQL Analytics",
        [
            sys.executable,
            "sql/run_analytics.py",
        ],
    ),
]


def main():
    pipeline_start = time.perf_counter()
    record_pipeline_status("RUNNING", 0)

    print("=" * 55)
    print("RETAIL SUPPLY CHAIN DATA PIPELINE")
    print("=" * 55)

    for step_name, command in STEPS:
        print(f"\nStarting: {step_name}")
        step_start = time.perf_counter()

        try:
            result = subprocess.run(command, cwd=PROJECT_ROOT)
        except Exception as exc:
            elapsed = time.perf_counter() - pipeline_start
            record_pipeline_status(
                "FAILED",
                elapsed,
                failed_step=step_name,
            )
            print(f"FAILED: {step_name}: {exc}")
            sys.exit(1)

        step_duration = time.perf_counter() - step_start

        if result.returncode != 0:
            elapsed = time.perf_counter() - pipeline_start
            record_pipeline_status(
                "FAILED",
                elapsed,
                failed_step=step_name,
            )
            print(f"\nFAILED: {step_name}")
            print(f"Exit code: {result.returncode}")
            sys.exit(result.returncode)

        print(f"PASSED: {step_name} ({step_duration:.2f} seconds)")

    total_duration = time.perf_counter() - pipeline_start
    record_pipeline_status("PASSED", total_duration)

    print("\n" + "=" * 55)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print(f"Total execution time: {total_duration:.2f} seconds")
    print("=" * 55)


if __name__ == "__main__":
    main()