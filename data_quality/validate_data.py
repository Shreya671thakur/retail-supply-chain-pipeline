from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("RetailSupplyChainDataQuality")
    .master("local[*]")
    .getOrCreate()
)

input_path = "./output/analytical_dataset_parquet"
quarantine_path = "./output/quarantine"

df = spark.read.parquet(input_path)

print("====================================")
print("DATA QUALITY VALIDATION")
print("====================================")

total_records = df.count()

print(f"Total records: {total_records}")


# -----------------------------
# Define invalid records
# -----------------------------

invalid_condition = (
    col("shipment_id").isNull()
    | col("product_id").isNull()
    | (col("quantity_shipped") < 0)
    | (col("total_shipment_value") < 0)
)

invalid_df = df.filter(invalid_condition)

valid_df = df.filter(~invalid_condition)


# -----------------------------
# Calculate metrics
# -----------------------------

invalid_records = invalid_df.count()
valid_records = valid_df.count()

print(f"Valid records: {valid_records}")
print(f"Invalid records: {invalid_records}")


# -----------------------------
# Duplicate check
# -----------------------------

duplicate_shipments = (
    df.groupBy("shipment_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print(f"Duplicate shipment IDs: {duplicate_shipments}")


# -----------------------------
# Save invalid records
# -----------------------------

if invalid_records > 0:
    print("Writing invalid records to quarantine...")

    invalid_df.write.mode("overwrite").parquet(
        quarantine_path
    )

    print(f"Quarantine location: {quarantine_path}")

else:
    print("No invalid records found.")


# -----------------------------
# Final status
# -----------------------------

print("====================================")

if invalid_records > 0 or duplicate_shipments > 0:
    print("DATA QUALITY STATUS: FAILED")
    print("Invalid records have been quarantined.")

    spark.stop()

    raise ValueError(
        "Data quality validation failed."
    )

else:
    print("DATA QUALITY STATUS: PASSED")

print("====================================")

spark.stop()