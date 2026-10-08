from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("RetailSupplyChainAnalytics")
    .master("local[*]")
    .getOrCreate()
)

input_path = "./output/analytical_dataset_parquet"

df = spark.read.parquet(input_path)

df.createOrReplaceTempView("shipments")


print("====================================")
print("SQL ANALYTICS")
print("====================================")


# 1. Top suppliers by shipment value

print("\nTop Suppliers by Shipment Value")

top_suppliers = spark.sql("""
    SELECT
        supplier_name,
        ROUND(SUM(total_shipment_value), 2) AS total_value
    FROM shipments
    GROUP BY supplier_name
    ORDER BY total_value DESC
    LIMIT 10
""")

top_suppliers.show(truncate=False)


# 2. Delayed shipments

print("\nDelayed Shipments")

delayed_shipments = spark.sql("""
    SELECT
        COUNT(*) AS delayed_shipments
    FROM shipments
    WHERE is_delayed = true
""")

delayed_shipments.show()

# 3. Average delivery delay

print("\nAverage Delivery Delay")

average_delay = spark.sql("""
    SELECT
        ROUND(AVG(delay_days), 2) AS average_delay_days
    FROM shipments
    WHERE is_delayed = true
""")

average_delay.show()

# 4. Low stock products

print("\nLow Stock Products")

low_stock = spark.sql("""
    SELECT
        product_id,
        product_name,
        stock_quantity,
        reorder_level
    FROM shipments
    WHERE is_below_reorder_level = true
    ORDER BY stock_quantity
    LIMIT 20
""")

low_stock.show(truncate=False)

# 5. Shipment status breakdown

print("\nShipment Status Breakdown")

status_breakdown = spark.sql("""
    SELECT
        status,
        COUNT(*) AS shipment_count
    FROM shipments
    GROUP BY status
    ORDER BY shipment_count DESC
""")

status_breakdown.show()


# 6. Monthly shipment volume

print("\nMonthly Shipment Volume")

monthly_shipments = spark.sql("""
    SELECT
        shipment_year,
        shipment_month,
        COUNT(*) AS shipment_count
    FROM shipments
    GROUP BY
        shipment_year,
        shipment_month
    ORDER BY
        shipment_year,
        shipment_month
""")

monthly_shipments.show(50)


spark.stop()