-- =============================================================================
-- analytics.sql
-- Retail Supply Chain Data Pipeline -- Analytics Queries
--
-- These queries run against the final analytical table produced by
-- pyspark/transform.py (table name: analytical_dataset).
--
-- Compatible with PostgreSQL and Spark SQL (minor dialect notes inline).
-- Load the analytical_dataset_csv / analytical_dataset_parquet output into
-- a table named `analytical_dataset` before running these queries, e.g.:
--
--   CREATE TABLE analytical_dataset (
--       shipment_id            VARCHAR(20),
--       inventory_id           VARCHAR(20),
--       product_id             VARCHAR(20),
--       product_name           VARCHAR(150),
--       category                VARCHAR(50),
--       supplier_id             VARCHAR(20),
--       supplier_name           VARCHAR(150),
--       supplier_category       VARCHAR(50),
--       country                 VARCHAR(50),
--       city                    VARCHAR(50),
--       rating                  NUMERIC(3,1),
--       warehouse_location      VARCHAR(30),
--       destination_warehouse   VARCHAR(30),
--       stock_quantity          INTEGER,
--       reorder_level           INTEGER,
--       is_below_reorder_level  BOOLEAN,
--       unit_price              NUMERIC(10,2),
--       quantity_shipped        INTEGER,
--       total_shipment_value    NUMERIC(12,2),
--       shipping_cost           NUMERIC(10,2),
--       shipment_date           DATE,
--       shipment_year           INTEGER,
--       shipment_month          INTEGER,
--       expected_delivery_date  DATE,
--       actual_delivery_date    DATE,
--       delay_days              INTEGER,
--       is_delayed              BOOLEAN,
--       status                  VARCHAR(20)
--   );
--
--   COPY analytical_dataset FROM '/path/to/analytical_dataset_csv/part-00000....csv'
--   WITH (FORMAT csv, HEADER true);
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. TOP SUPPLIERS
-- Ranks suppliers by total shipment value delivered, along with their
-- average rating and on-time delivery rate.
-- -----------------------------------------------------------------------------
SELECT
    supplier_id,
    supplier_name,
    country,
    ROUND(AVG(rating)::numeric, 2)                                   AS avg_rating,
    COUNT(DISTINCT shipment_id)                                      AS total_shipments,
    SUM(total_shipment_value)                                        AS total_shipment_value,
    ROUND(
        100.0 * SUM(CASE WHEN is_delayed = FALSE AND status = 'DELIVERED' THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END), 0)
    , 2)                                                              AS on_time_delivery_pct
FROM analytical_dataset
GROUP BY supplier_id, supplier_name, country
ORDER BY total_shipment_value DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- 2. DELAYED SHIPMENTS
-- Lists all shipments that arrived later than their expected delivery date,
-- with the magnitude of the delay and the responsible supplier.
-- -----------------------------------------------------------------------------
SELECT
    shipment_id,
    supplier_id,
    supplier_name,
    product_name,
    warehouse_location,
    destination_warehouse,
    shipment_date,
    expected_delivery_date,
    actual_delivery_date,
    delay_days,
    status
FROM analytical_dataset
WHERE is_delayed = TRUE
ORDER BY delay_days DESC;


-- 2b. Delay summary per supplier (count + average delay)
SELECT
    supplier_id,
    supplier_name,
    COUNT(*)                               AS delayed_shipment_count,
    ROUND(AVG(delay_days)::numeric, 2)     AS avg_delay_days,
    MAX(delay_days)                        AS max_delay_days
FROM analytical_dataset
WHERE is_delayed = TRUE
GROUP BY supplier_id, supplier_name
ORDER BY delayed_shipment_count DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- 3. INVENTORY TURNOVER
-- Approximates turnover by comparing total quantity shipped against current
-- stock quantity per product. Higher ratio = faster-moving inventory.
-- -----------------------------------------------------------------------------
SELECT
    inventory_id,
    product_id,
    product_name,
    category,
    MAX(stock_quantity)                                  AS current_stock_quantity,
    SUM(quantity_shipped)                                AS total_quantity_shipped,
    ROUND(
        SUM(quantity_shipped)::numeric / NULLIF(MAX(stock_quantity), 0)
    , 2)                                                  AS inventory_turnover_ratio,
    MAX(reorder_level)                                   AS reorder_level,
    BOOL_OR(is_below_reorder_level)                      AS currently_below_reorder_level
FROM analytical_dataset
GROUP BY inventory_id, product_id, product_name, category
ORDER BY inventory_turnover_ratio DESC
LIMIT 20;


-- 3b. Low-stock alert: products at/below reorder level
SELECT DISTINCT
    inventory_id,
    product_name,
    category,
    warehouse_location,
    stock_quantity,
    reorder_level
FROM analytical_dataset
WHERE is_below_reorder_level = TRUE
ORDER BY stock_quantity ASC;


-- -----------------------------------------------------------------------------
-- 4. MONTHLY SHIPMENT VOLUME
-- Aggregates shipment counts, quantities, and value by calendar month.
-- -----------------------------------------------------------------------------
SELECT
    shipment_year,
    shipment_month,
    COUNT(shipment_id)                  AS total_shipments,
    SUM(quantity_shipped)               AS total_units_shipped,
    SUM(total_shipment_value)           AS total_shipment_value,
    ROUND(AVG(shipping_cost)::numeric, 2) AS avg_shipping_cost,
    SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) AS delayed_shipments
FROM analytical_dataset
GROUP BY shipment_year, shipment_month
ORDER BY shipment_year, shipment_month;


-- 4b. Monthly shipment volume by category (for trend dashboards)
SELECT
    shipment_year,
    shipment_month,
    category,
    COUNT(shipment_id)            AS total_shipments,
    SUM(total_shipment_value)     AS total_shipment_value
FROM analytical_dataset
GROUP BY shipment_year, shipment_month, category
ORDER BY shipment_year, shipment_month, total_shipment_value DESC;


-- -----------------------------------------------------------------------------
-- 5. BONUS: Shipment status breakdown (operational health check)
-- -----------------------------------------------------------------------------
SELECT
    status,
    COUNT(*)                                       AS shipment_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM analytical_dataset
GROUP BY status
ORDER BY shipment_count DESC;
