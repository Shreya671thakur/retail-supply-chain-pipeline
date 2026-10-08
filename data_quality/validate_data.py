from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("RetailSupplyChainDataQuality")
    .master("local[*]")
    .getOrCreate()
)

input_path = "./output/analytical_dataset_parquet"

df = spark.read.parquet(input_path)

print("====================================")
print("DATA QUALITY CHECKS")
print("====================================")

print(f"Total rows: {df.count()}")

# Check 1: shipment_id should not be null
null_shipment_ids = df.filter(
    col("shipment_id").isNull()
).count()

print(f"Null shipment IDs: {null_shipment_ids}")

# Check 2: product_id should not be null
null_product_ids = df.filter(
    col("product_id").isNull()
).count()

print(f"Null product IDs: {null_product_ids}")

# Check 3: quantity shipped should not be negative
negative_quantity = df.filter(
    col("quantity_shipped") < 0
).count()

print(f"Negative quantities: {negative_quantity}")

# Check 4: shipment value should not be negative
negative_value = df.filter(
    col("total_shipment_value") < 0
).count()

print(f"Negative shipment values: {negative_value}")

# Check 5: duplicate shipment IDs
duplicate_shipments = (
    df.groupBy("shipment_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

total_errors = (
    null_shipment_ids
    + null_product_ids
    + negative_quantity
    + negative_value
    + duplicate_shipments
)

print("====================================")
print(f"Total data quality errors: {total_errors}")
print("====================================")

if total_errors > 0:
    print("DATA QUALITY STATUS: FAILED")
    spark.stop()
    raise ValueError("Data quality validation failed.")

print("DATA QUALITY STATUS: PASSED")

spark.stop()