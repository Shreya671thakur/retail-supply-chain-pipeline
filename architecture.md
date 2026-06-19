# Architecture -- Retail Supply Chain Data Pipeline

## 1. Overview

This project implements a **batch ETL/ELT pipeline** that ingests raw retail
supply chain data (suppliers, inventory, shipments), cleans and joins it
using **PySpark**, orchestrates the workflow with **Apache Airflow**, and
serves analytics through **SQL** queries on the final dataset.

The design follows a simplified **Bronze → Silver → Gold** layering pattern
commonly used in modern data lakehouse architectures:

| Layer  | Description                                   | Location                              |
|--------|------------------------------------------------|----------------------------------------|
| Bronze | Raw, unmodified source CSVs staged for a run   | `staging/bronze/`                      |
| Silver | Cleaned, deduplicated, standardized DataFrames | in-memory during Spark job             |
| Gold   | Final joined analytical dataset                | `output/analytical_dataset_parquet/`, `output/analytical_dataset_csv/` |

## 2. High-Level Data Flow

```
            ┌────────────────────┐
            │   Source Systems     │
            │ suppliers.csv         │
            │ inventory.csv         │
            │ shipments.csv          │
            └──────────┬───────────┘
                       │
                       ▼
            ┌────────────────────┐
            │   Airflow DAG         │
            │ retail_supply_chain_  │
            │ pipeline               │
            └──────────┬───────────┘
                       │
       ┌───────────────┼────────────────┐
       ▼                ▼                 ▼
 check_source_files  ingest_raw_data  run_spark_transform
                                          │
                                          ▼
                                 ┌──────────────────┐
                                 │  PySpark Job        │
                                 │  transform.py        │
                                 │  - clean nulls         │
                                 │  - dedupe                │
                                 │  - standardize columns    │
                                 │  - join 3 datasets          │
                                 └──────────┬───────────┘
                                            │
                                            ▼
                                 ┌──────────────────┐
                                 │ Analytical Dataset   │
                                 │ (Parquet + CSV)        │
                                 └──────────┬───────────┘
                                            │
                                            ▼
                                 ┌──────────────────┐
                                 │ run_sql_analytics      │
                                 │ - load to Postgres       │
                                 │ - execute analytics.sql    │
                                 └──────────┬───────────┘
                                            │
                                            ▼
                                 ┌──────────────────┐
                                 │ generate_final_report   │
                                 │ -> output/pipeline_run_   │
                                 │    report.txt                │
                                 └──────────┬───────────┘
                                            │
                                            ▼
                                  notify_completion
```

## 3. Component Responsibilities

### 3.1 Datasets (`datasets/`)
Synthetic but realistic CSV files representing the three core retail supply
chain entities:
- **suppliers.csv** -- supplier master data (100 rows)
- **inventory.csv** -- per-product stock data tied to a supplier (1,000 rows)
- **shipments.csv** -- shipment/delivery transactions tied to inventory (2,000 rows)

Intentional data quality issues (missing values, a small percentage of
duplicate rows, inconsistent casing) are injected to make the cleaning step
meaningful, just like real-world source extracts.

### 3.2 PySpark Transformation (`pyspark/transform.py`)
A single Spark batch job responsible for:
1. **Extract** -- read all 3 CSVs with explicit schemas.
2. **Clean** -- trim strings, fill/standardize nulls, fix casing, cast dates.
3. **Deduplicate** -- `dropDuplicates` on each entity's primary key.
4. **Standardize** -- consistent column naming, type casting, derived flags
   (e.g. `is_below_reorder_level`, `is_delayed`).
5. **Join** -- `shipments → inventory → suppliers` to build a single wide,
   analytics-ready fact table.
6. **Load** -- write the result to both Parquet (efficient downstream reads)
   and CSV (human-readable / easy DB loading).

### 3.3 Airflow DAG (`airflow/retail_pipeline_dag.py`)
Orchestrates the pipeline as a 6-task DAG running on a daily schedule:

1. `check_source_files` -- validation gate; fails fast on missing/empty inputs.
2. `ingest_raw_data` -- stages raw files into a bronze landing zone with a row-count audit (simulates ingestion from an external system).
3. `run_spark_transform` -- `spark-submit`s `transform.py`.
4. `run_sql_analytics` -- loads the Gold dataset into PostgreSQL and executes `sql/analytics.sql` via `airflow/run_sql_analytics.py`.
5. `generate_final_report` -- writes a human-readable run summary.
6. `notify_completion` -- logs pipeline completion (swap for Slack/email in production).

### 3.4 SQL Analytics (`sql/analytics.sql`)
A set of standalone, parameter-free analytical queries against the Gold
table (`analytical_dataset`):
- Top suppliers by shipment value & on-time delivery rate
- Delayed shipments (detail + supplier-level summary)
- Inventory turnover ratio + low-stock alerts
- Monthly shipment volume (overall and by category)
- Bonus: shipment status breakdown

## 4. Technology Choices & Rationale

| Concern              | Choice          | Why                                                                 |
|-----------------------|------------------|----------------------------------------------------------------------|
| Orchestration          | Apache Airflow    | Industry-standard scheduler with clear DAG semantics, retries, and observability |
| Transformation engine    | PySpark             | Scales from a laptop (`local[*]`) to a cluster without code changes; robust DataFrame API for joins/cleaning |
| Storage format (Gold)      | Parquet + CSV         | Parquet for efficient columnar analytics; CSV for portability into any SQL engine |
| Analytics layer               | PostgreSQL (optional)    | Familiar SQL interface; analytics.sql also runs unmodified on Spark SQL / DuckDB |
| Data generation                  | Python `random`/`csv`       | Deterministic (seeded), dependency-free synthetic data generation |

## 5. Idempotency & Reliability

- `transform.py` always writes with `mode("overwrite")`, so re-running a DAG
  run for the same logical date produces the same Gold output (idempotent).
- `run_sql_analytics.py` truncates the target table before loading, avoiding
  duplicate accumulation on retries.
- Airflow's `retries=2` with a 5-minute delay (see `default_args`) handles
  transient failures (e.g., Spark executor hiccups).

## 6. Scaling Path (Future Architecture)

```
Local CSV  →  S3 / ADLS (raw zone)
Local Spark  →  EMR / Databricks / Glue
Local Postgres  →  Snowflake / BigQuery / Redshift
Airflow standalone  →  Managed Airflow (MWAA / Astronomer / Cloud Composer)
```

See `README.md` → "Future Improvements" for the full roadmap.
