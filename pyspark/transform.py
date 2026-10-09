"""
transform.py
------------
PySpark batch transformation job for the Retail Supply Chain Data Pipeline.

Steps:
 1. Read raw CSVs (suppliers, inventory, shipments)
 2. Clean null / missing values
 3. Remove duplicate records
 4. Standardize column names, data types, and string formatting
 5. Join supplier + inventory + shipment data
 6. Write a final analytical dataset (Parquet + CSV) for downstream SQL analytics

Usage:
    spark-submit transform.py \
        --input-dir ../datasets \
        --output-dir ../output

Can also be run with plain `python transform.py` if PySpark is installed
locally (it will boot an embedded local[*] SparkSession).
"""

import argparse
import os
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
def get_spark(app_name: str = "RetailSupplyChainPipeline") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Schemas (explicit schemas avoid bad type inference on messy CSVs)
# ---------------------------------------------------------------------------
SUPPLIERS_SCHEMA = StructType([
    StructField("supplier_id", StringType(), True),
    StructField("supplier_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("contact_email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("rating", DoubleType(), True),
    StructField("onboarded_date", StringType(), True),
])

INVENTORY_SCHEMA = StructType([
    StructField("inventory_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("supplier_id", StringType(), True),
    StructField("warehouse_location", StringType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("reorder_level", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("last_restock_date", StringType(), True),
])

SHIPMENTS_SCHEMA = StructType([
    StructField("shipment_id", StringType(), True),
    StructField("inventory_id", StringType(), True),
    StructField("supplier_id", StringType(), True),
    StructField("shipment_date", StringType(), True),
    StructField("expected_delivery_date", StringType(), True),
    StructField("actual_delivery_date", StringType(), True),
    StructField("quantity_shipped", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("destination_warehouse", StringType(), True),
    StructField("shipping_cost", DoubleType(), True),
])


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------
def read_csv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    return (
        spark.read
        .option("header", "true")
        .option("nullValue", "")
        .schema(schema)
        .csv(path)
    )


# ---------------------------------------------------------------------------
# Clean / standardize helpers
# ---------------------------------------------------------------------------
def standardize_columns(df: DataFrame) -> DataFrame:
    """Lowercase + snake_case all column names (defensive, in case source
    columns ever arrive with mixed casing or stray whitespace)."""
    for c in df.columns:
        new_c = c.strip().lower().replace(" ", "_")
        if new_c != c:
            df = df.withColumnRenamed(c, new_c)
    return df


def trim_string_columns(df: DataFrame) -> DataFrame:
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in string_cols:
        df = df.withColumn(c, F.trim(F.col(c)))
    return df


def clean_suppliers(df: DataFrame) -> DataFrame:
    df = standardize_columns(df)
    df = trim_string_columns(df)
    df = df.dropDuplicates(["supplier_id"])
    df = df.filter(F.col("supplier_id").isNotNull())

    df = (
        df
        .withColumn("rating", F.when(F.col("rating").isNull(), F.lit(3.0)).otherwise(F.col("rating")))
        .withColumn("contact_email", F.when(F.col("contact_email").isNull(), F.lit("unknown@supplier.com")).otherwise(F.col("contact_email")))
        .withColumn("phone", F.when(F.col("phone").isNull(), F.lit("N/A")).otherwise(F.col("phone")))
        .withColumn("onboarded_date", F.to_date("onboarded_date", "yyyy-MM-dd"))
        .withColumn("supplier_name", F.initcap(F.col("supplier_name")))
        .withColumn("country", F.initcap(F.col("country")))
        .withColumn("city", F.initcap(F.col("city")))
    )
    return df


def clean_inventory(df: DataFrame) -> DataFrame:
    df = standardize_columns(df)
    df = trim_string_columns(df)
    df = df.dropDuplicates(["inventory_id"])
    df = df.filter(F.col("inventory_id").isNotNull() & F.col("supplier_id").isNotNull())

    df = (
        df
        .withColumn("stock_quantity", F.when(F.col("stock_quantity").isNull(), F.lit(0)).otherwise(F.col("stock_quantity")))
        .withColumn("unit_price", F.when(F.col("unit_price").isNull(), F.lit(0.0)).otherwise(F.col("unit_price")))
        .withColumn("warehouse_location", F.when(F.col("warehouse_location").isNull(), F.lit("UNKNOWN")).otherwise(F.col("warehouse_location")))
        .withColumn("last_restock_date", F.to_date("last_restock_date", "yyyy-MM-dd"))
        .withColumn("product_name", F.initcap(F.col("product_name")))
        .withColumn("is_below_reorder_level", F.col("stock_quantity") < F.col("reorder_level"))
    )
    return df


def clean_shipments(df: DataFrame) -> DataFrame:
    df = standardize_columns(df)
    df = trim_string_columns(df)
    df = df.dropDuplicates(["shipment_id"])
    df = df.filter(F.col("shipment_id").isNotNull() & F.col("inventory_id").isNotNull())

    df = (
        df
        .withColumn("shipping_cost", F.when(F.col("shipping_cost").isNull(), F.lit(0.0)).otherwise(F.col("shipping_cost")))
        .withColumn("destination_warehouse", F.when(F.col("destination_warehouse").isNull(), F.lit("UNKNOWN")).otherwise(F.col("destination_warehouse")))
        .withColumn("status", F.upper(F.col("status")))
        .withColumn("shipment_date", F.to_date("shipment_date", "yyyy-MM-dd"))
        .withColumn("expected_delivery_date", F.to_date("expected_delivery_date", "yyyy-MM-dd"))
        .withColumn("actual_delivery_date", F.to_date("actual_delivery_date", "yyyy-MM-dd"))
        .withColumn(
            "delay_days",
            F.when(
                F.col("actual_delivery_date").isNotNull(),
                F.datediff(F.col("actual_delivery_date"), F.col("expected_delivery_date")),
            ).otherwise(F.lit(None).cast(IntegerType())),
        )
        .withColumn(
            "is_delayed",
            F.when(F.col("delay_days") > 0, F.lit(True)).otherwise(F.lit(False)),
        )
    )
    return df


# ---------------------------------------------------------------------------
# Transform / Join
# ---------------------------------------------------------------------------
def build_analytical_dataset(suppliers: DataFrame, inventory: DataFrame, shipments: DataFrame) -> DataFrame:
    inv_sup = inventory.join(
        suppliers.select(
            "supplier_id", "supplier_name", "category", "country", "city", "rating"
        ).withColumnRenamed("category", "supplier_category"),
        on="supplier_id",
        how="left",
    )

    final_df = shipments.join(
        inv_sup,
        on="inventory_id",
        how="left",
        # shipments.supplier_id may occasionally differ from inventory's
        # owning supplier (noisy source data) -- keep shipment's own id
    ).drop(inv_sup["supplier_id"]) \
     .withColumnRenamed("supplier_id", "supplier_id")

    final_df = (
        final_df
        .withColumn("total_shipment_value", F.round(F.col("quantity_shipped") * F.col("unit_price"), 2))
        .withColumn("shipment_year", F.year("shipment_date"))
        .withColumn("shipment_month", F.month("shipment_date"))
        .select(
            "shipment_id",
            "inventory_id",
            "product_id",
            "product_name",
            "category",
            "supplier_id",
            "supplier_name",
            "supplier_category",
            "country",
            "city",
            "rating",
            "warehouse_location",
            "destination_warehouse",
            "stock_quantity",
            "reorder_level",
            "is_below_reorder_level",
            "unit_price",
            "quantity_shipped",
            "total_shipment_value",
            "shipping_cost",
            "shipment_date",
            "shipment_year",
            "shipment_month",
            "expected_delivery_date",
            "actual_delivery_date",
            "delay_days",
            "is_delayed",
            "status",
        )
    )
    return final_df


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
def write_outputs(df: DataFrame, output_dir: str):
    parquet_path = os.path.join(output_dir, "analytical_dataset_parquet")
    csv_path = os.path.join(output_dir, "analytical_dataset_csv")

    (
        df.write
        .mode("overwrite")
        .partitionBy("shipment_year", "shipment_month")
        .parquet(parquet_path)
    )
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)

    print(f"[transform.py] Parquet written to: {parquet_path}")
    print(f"[transform.py] CSV written to:     {csv_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Retail Supply Chain PySpark transformation job")
    parser.add_argument("--input-dir", default="../datasets", help="Directory containing raw CSV files")
    parser.add_argument("--output-dir", default="../output", help="Directory to write transformed output")
    args = parser.parse_args()

    spark = get_spark()

    suppliers_raw = read_csv(spark, os.path.join(args.input_dir, "suppliers.csv"), SUPPLIERS_SCHEMA)
    inventory_raw = read_csv(spark, os.path.join(args.input_dir, "inventory.csv"), INVENTORY_SCHEMA)
    shipments_raw = read_csv(spark, os.path.join(args.input_dir, "shipments.csv"), SHIPMENTS_SCHEMA)

    print(f"[transform.py] Raw row counts -> suppliers={suppliers_raw.count()}, "
          f"inventory={inventory_raw.count()}, shipments={shipments_raw.count()}")

    suppliers = clean_suppliers(suppliers_raw)
    inventory = clean_inventory(inventory_raw)
    shipments = clean_shipments(shipments_raw)

    print(f"[transform.py] Clean row counts -> suppliers={suppliers.count()}, "
          f"inventory={inventory.count()}, shipments={shipments.count()}")

    final_df = build_analytical_dataset(suppliers, inventory, shipments)
    final_df.cache()

    print(f"[transform.py] Final analytical dataset row count: {final_df.count()}")
    final_df.show(10, truncate=False)

    os.makedirs(args.output_dir, exist_ok=True)
    write_outputs(final_df, args.output_dir)

    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
