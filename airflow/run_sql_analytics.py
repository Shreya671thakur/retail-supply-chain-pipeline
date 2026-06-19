"""
run_sql_analytics.py
---------------------
Helper script invoked by the Airflow `run_sql_analytics` task.

It:
  1. Loads the Spark-produced analytical_dataset CSV into a PostgreSQL table.
  2. Executes each query in sql/analytics.sql against that table.
  3. Writes the results of each query to output/analytics_results/<n>_<name>.csv

This script is optional/standalone -- if PostgreSQL is not available, the
Airflow task can be swapped for a DuckDB or Spark SQL equivalent (see
README.md "Running without PostgreSQL").

Requires: psycopg2-binary, pandas
    pip install psycopg2-binary pandas
"""

import argparse
import glob
import os
import re
import sys

import pandas as pd
import psycopg2


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    shipment_id            VARCHAR(20),
    inventory_id           VARCHAR(20),
    product_id             VARCHAR(20),
    product_name           VARCHAR(150),
    category                VARCHAR(50),
    supplier_id             VARCHAR(20),
    supplier_name           VARCHAR(150),
    supplier_category       VARCHAR(50),
    country                 VARCHAR(50),
    city                    VARCHAR(50),
    rating                  NUMERIC(3,1),
    warehouse_location      VARCHAR(30),
    destination_warehouse   VARCHAR(30),
    stock_quantity          INTEGER,
    reorder_level           INTEGER,
    is_below_reorder_level  BOOLEAN,
    unit_price              NUMERIC(10,2),
    quantity_shipped        INTEGER,
    total_shipment_value    NUMERIC(12,2),
    shipping_cost           NUMERIC(10,2),
    shipment_date           DATE,
    shipment_year           INTEGER,
    shipment_month          INTEGER,
    expected_delivery_date  DATE,
    actual_delivery_date    DATE,
    delay_days              INTEGER,
    is_delayed              BOOLEAN,
    status                  VARCHAR(20)
);
"""


def get_pg_connection(conn_id: str):
    """Resolve connection params from environment variables for simplicity.
    In a real Airflow deployment, swap this for:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=conn_id)
        return hook.get_conn()
    """
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=os.environ.get("PG_PORT", "5432"),
        dbname=os.environ.get("PG_DATABASE", "retail_pipeline"),
        user=os.environ.get("PG_USER", "postgres"),
        password=os.environ.get("PG_PASSWORD", "postgres"),
    )


def find_spark_csv(output_dir: str) -> str:
    pattern = os.path.join(output_dir, "analytical_dataset_csv", "part-*.csv")
    matches = glob.glob(pattern)
    if not matches:
        raise FileNotFoundError(f"No Spark CSV output found at {pattern}")
    return matches[0]


def load_into_postgres(csv_path: str, table: str, conn_id: str):
    conn = get_pg_connection(conn_id)
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL.format(table=table))
            cur.execute(f"TRUNCATE TABLE {table};")
            with open(csv_path, "r", encoding="utf-8") as f:
                cur.copy_expert(
                    f"COPY {table} FROM STDIN WITH (FORMAT csv, HEADER true)", f
                )
        conn.commit()
        print(f"[run_sql_analytics] Loaded {csv_path} into table '{table}'")
    finally:
        conn.close()


def split_statements(sql_text: str):
    """Naive split on semicolons, ignoring -- comment-only lines."""
    cleaned_lines = [
        line for line in sql_text.splitlines()
        if not line.strip().startswith("--")
    ]
    cleaned = "\n".join(cleaned_lines)
    statements = [s.strip() for s in cleaned.split(";") if s.strip()]
    return statements


def run_analytics(sql_file: str, output_dir: str, conn_id: str):
    with open(sql_file, "r", encoding="utf-8") as f:
        sql_text = f.read()

    statements = split_statements(sql_text)
    results_dir = os.path.join(output_dir, "analytics_results")
    os.makedirs(results_dir, exist_ok=True)

    conn = get_pg_connection(conn_id)
    try:
        for i, stmt in enumerate(statements, start=1):
            df = pd.read_sql_query(stmt, conn)
            out_name = f"{i:02d}_query_result.csv"
            out_path = os.path.join(results_dir, out_name)
            df.to_csv(out_path, index=False)
            print(f"[run_sql_analytics] Query {i}: {len(df)} rows -> {out_path}")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--sql-file", required=True)
    parser.add_argument("--conn-id", default="retail_postgres")
    parser.add_argument("--table", default="analytical_dataset")
    args = parser.parse_args()

    csv_path = find_spark_csv(args.output_dir)
    load_into_postgres(csv_path, args.table, args.conn_id)
    run_analytics(args.sql_file, args.output_dir, args.conn_id)


if __name__ == "__main__":
    sys.exit(main())
