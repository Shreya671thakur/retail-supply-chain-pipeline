"""
retail_pipeline_dag.py
-----------------------
Apache Airflow DAG for the Retail Supply Chain Data Pipeline.

Pipeline stages:
    1. check_source_files     -> verify suppliers/inventory/shipments CSVs exist
    2. ingest_raw_data         -> copy/stage raw files into a working "bronze" zone
    3. run_spark_transform     -> spark-submit pyspark/transform.py (clean, dedupe, join)
    4. run_sql_analytics       -> load analytical dataset into Postgres + run analytics.sql
    5. generate_final_report   -> summarize results into a final output file
    6. notify_completion       -> log/print pipeline completion summary

Schedule: daily batch run (@daily). Adjust `schedule_interval` as needed.

To run locally:
    export AIRFLOW_HOME=~/airflow
    airflow db init
    cp retail_pipeline_dag.py $AIRFLOW_HOME/dags/
    airflow standalone
"""

from datetime import datetime, timedelta
import csv
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------------------
# Path configuration -- adjust PROJECT_ROOT to match your local checkout
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.environ.get(
    "RETAIL_PIPELINE_ROOT",
    "/opt/airflow/projects/retail-supply-chain-pipeline",
)

DATASETS_DIR = os.path.join(PROJECT_ROOT, "datasets")
BRONZE_DIR = os.path.join(PROJECT_ROOT, "staging", "bronze")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
SPARK_SCRIPT = os.path.join(PROJECT_ROOT, "pyspark", "transform.py")
SQL_SCRIPT = os.path.join(PROJECT_ROOT, "sql", "analytics.sql")
REPORT_PATH = os.path.join(OUTPUT_DIR, "pipeline_run_report.txt")

REQUIRED_FILES = ["suppliers.csv", "inventory.csv", "shipments.csv"]

POSTGRES_CONN_ID = "retail_postgres"  # configure via Airflow Connections UI
ANALYTICAL_TABLE = "analytical_dataset"


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Python callables
# ---------------------------------------------------------------------------
def check_source_files(**context):
    """Fail fast if any of the raw source CSVs are missing or empty."""
    missing = []
    for filename in REQUIRED_FILES:
        path = os.path.join(DATASETS_DIR, filename)
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            missing.append(filename)
    if missing:
        raise FileNotFoundError(f"Missing or empty source files: {missing}")
    print(f"[check_source_files] All required files present in {DATASETS_DIR}")


def ingest_raw_data(**context):
    """Stage raw files into a bronze/ landing zone with a row-count audit log.
    In a production setup this might instead pull from S3/SFTP/an API."""
    os.makedirs(BRONZE_DIR, exist_ok=True)
    audit = {}
    for filename in REQUIRED_FILES:
        src = os.path.join(DATASETS_DIR, filename)
        dst = os.path.join(BRONZE_DIR, filename)
        with open(src, "r", encoding="utf-8") as f_in:
            reader = csv.reader(f_in)
            rows = list(reader)
        with open(dst, "w", encoding="utf-8", newline="") as f_out:
            writer = csv.writer(f_out)
            writer.writerows(rows)
        audit[filename] = len(rows) - 1  # exclude header
    print(f"[ingest_raw_data] Staged files to {BRONZE_DIR} -> row counts: {audit}")
    context["ti"].xcom_push(key="ingest_audit", value=audit)


def generate_final_report(**context):
    """Summarize the pipeline run into a human-readable report file."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    audit = context["ti"].xcom_pull(key="ingest_audit", task_ids="ingest_raw_data")

    lines = [
        "Retail Supply Chain Pipeline -- Run Report",
        "=" * 50,
        f"Run timestamp: {datetime.now().isoformat()}",
        f"Source row counts: {audit}",
        f"Spark output directory: {OUTPUT_DIR}",
        "Status: SUCCESS",
    ]
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"[generate_final_report] Report written to {REPORT_PATH}")


def notify_completion(**context):
    print("[notify_completion] Retail Supply Chain Pipeline completed successfully. ✅")
    print(f"[notify_completion] Final report available at: {REPORT_PATH}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="retail_supply_chain_pipeline",
    description="End-to-end batch pipeline for supplier, inventory, and shipment data",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["retail", "supply-chain", "batch", "pyspark", "sql"],
) as dag:

    t1_check_source_files = PythonOperator(
        task_id="check_source_files",
        python_callable=check_source_files,
    )

    t2_ingest_raw_data = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=ingest_raw_data,
    )

    t3_run_spark_transform = BashOperator(
        task_id="run_spark_transform",
        bash_command=(
            f"spark-submit "
            f"--master local[*] "
            f"{SPARK_SCRIPT} "
            f"--input-dir {BRONZE_DIR} "
            f"--output-dir {OUTPUT_DIR}"
        ),
    )

    # Loads the Spark CSV output into Postgres, then executes analytics.sql.
    # Requires: pip install psycopg2-binary, and an Airflow connection
    # named `retail_postgres` configured in the Airflow UI / CLI.
    t4_run_sql_analytics = BashOperator(
        task_id="run_sql_analytics",
        bash_command=(
            f"python3 {PROJECT_ROOT}/airflow/run_sql_analytics.py "
            f"--output-dir {OUTPUT_DIR} "
            f"--sql-file {SQL_SCRIPT} "
            f"--conn-id {POSTGRES_CONN_ID} "
            f"--table {ANALYTICAL_TABLE}"
        ),
    )

    t5_generate_final_report = PythonOperator(
        task_id="generate_final_report",
        python_callable=generate_final_report,
    )

    t6_notify_completion = PythonOperator(
        task_id="notify_completion",
        python_callable=notify_completion,
    )

    # ------------------------------------------------------------------
    # Task dependency chain
    # ------------------------------------------------------------------
    (
        t1_check_source_files
        >> t2_ingest_raw_data
        >> t3_run_spark_transform
        >> t4_run_sql_analytics
        >> t5_generate_final_report
        >> t6_notify_completion
    )
