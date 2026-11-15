-- Analyst queries over the Iceberg tables, read from Snowflake.
-- Run in Snowsight, or `make snowflake-queries` to export each result to results/*.csv.
-- Each result-producing query is tagged with "-- name: <file name>".

USE WAREHOUSE NYC_TAXI_WH;
USE SCHEMA NYC_TAXI.ANALYTICS;

-- Timestamps are NYC local wall-clock values stored as UTC instants (see README
-- "Assumptions"). Reading them in UTC keeps the hour of day exactly as recorded.
ALTER SESSION SET TIMEZONE = 'UTC';

-- name: q1_top10_pickup_zones_by_revenue
-- Q1. Which 10 pickup zones earn the most revenue?
-- Revenue = total_amount (fare + tips + tolls + taxes + surcharges paid by the rider).
SELECT
    pickup_borough,
    pickup_zone,
    SUM(trips)                         AS trips,
    ROUND(SUM(total_revenue), 2)       AS total_revenue,
    ROUND(SUM(total_revenue) / SUM(trips), 2) AS revenue_per_trip
FROM daily_zone_metrics
GROUP BY pickup_borough, pickup_zone
ORDER BY total_revenue DESC
LIMIT 10;

-- name: q2_trip_demand_by_hour
-- Q2. How does trip demand vary by hour of day?
SELECT
    HOUR(pickup_datetime)                                   AS pickup_hour,
    COUNT(*)                                                AS trips,
    ROUND(COUNT(*) / COUNT(DISTINCT pickup_date), 0)        AS avg_trips_per_day,
    ROUND(100 * RATIO_TO_REPORT(COUNT(*)) OVER (), 2)       AS pct_of_all_trips
FROM trips_clean
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- name: q3_avg_tip_pct_by_payment_type
-- Q3. What is the average tip percentage by payment type?
-- Note: TLC only records card tips. Cash tips are not captured, so cash shows ~0%.
SELECT
    payment_type,
    CASE payment_type
        WHEN 0 THEN 'Flex fare'
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided trip'
        ELSE 'Other'
    END                               AS payment_type_desc,
    COUNT(*)                          AS trips,
    ROUND(AVG(tip_pct), 2)            AS avg_tip_pct,
    ROUND(MEDIAN(tip_pct), 2)         AS median_tip_pct
FROM trips_clean
GROUP BY payment_type
ORDER BY trips DESC;
