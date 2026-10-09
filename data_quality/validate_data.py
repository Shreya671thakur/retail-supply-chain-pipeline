import time
from utils.metrics import record_metrics
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from utils.logger import get_logger
from pipeline_metrics.record_metrics import record_metrics

logger = get_logger("DataQuality")

start_time = time.perf_counter()

spark = (
    SparkSession.builder
    .appName("RetailSupplyChainDataQuality")
    .master("local[*]")
    .getOrCreate()
)

input_path = "./output/analytical_dataset_parquet"
quarantine_path = "./output/quarantine"

pipeline_start_time = time.perf_counter()

total_records = 0
valid_records = 0
invalid_records = 0
duplicate_shipments = 0
status = "FAILED"

validation_duration = time.perf_counter() - pipeline_start_time

validation_status = (
    "PASSED"
    if invalid_records == 0 and duplicate_shipments == 0
    else "FAILED"
)

record_metrics(
    total_records=total_records,
    valid_records=valid_records,
    invalid_records=invalid_records,
    duplicate_shipments=duplicate_shipments,
    status=validation_status,
    duration_seconds=validation_duration,
)

try:
    df = spark.read.parquet(input_path)

    print("====================================")
    print("DATA QUALITY VALIDATION")
    print("====================================")

    total_records = df.count()
    logger.info(f"Total records: {total_records}")

    invalid_condition = (
        col("shipment_id").isNull()
        | col("product_id").isNull()
        | (col("quantity_shipped") < 0)
        | (col("total_shipment_value") < 0)
    )

    invalid_df = df.filter(invalid_condition)
    valid_df = df.filter(~invalid_condition)

    invalid_records = invalid_df.count()
    valid_records = valid_df.count()

    logger.info(f"Valid records: {valid_records}")
    logger.info(f"Invalid records: {invalid_records}")

    duplicate_shipments = (
        df.groupBy("shipment_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    logger.info(f"Duplicate shipment IDs: {duplicate_shipments}")

    if invalid_records > 0:
        invalid_df.write.mode("overwrite").parquet(quarantine_path)
        logger.warning(f"Invalid records saved to {quarantine_path}")

    if invalid_records > 0 or duplicate_shipments > 0:
        raise ValueError("Data quality validation failed.")

    status = "SUCCESS"
    logger.info("DATA QUALITY STATUS: PASSED")

finally:
    duration_seconds = time.perf_counter() - start_time

    try:
        record_metrics(
            total_records=total_records,
            valid_records=valid_records,
            invalid_records=invalid_records,
            duplicate_shipments=duplicate_shipments,
            status=status,
            duration_seconds=duration_seconds,
        )
    finally:
        spark.stop()