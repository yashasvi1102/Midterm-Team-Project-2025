-- Databricks notebook source
-- =======================================================
-- SILVER: Dallas Food Inspections (cleansed)
-- =======================================================

CREATE OR REFRESH LIVE TABLE silver_dallas_food_inspections (

  CONSTRAINT dallas_restaurant_not_null
    EXPECT (restaurant_name IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT dallas_inspection_date_not_null
    EXPECT (inspection_date IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT dallas_inspection_type_not_null
    EXPECT (inspection_type IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT dallas_zip_not_null
    EXPECT (zip IS NOT NULL)
    ON VIOLATION DROP ROW,

  CONSTRAINT dallas_zip_valid
    EXPECT (zip RLIKE '^[0-9]{5}$')
    ON VIOLATION DROP ROW,

  CONSTRAINT dallas_score_le_100
    EXPECT (inspection_score <= 100)
    ON VIOLATION DROP ROW
)
AS


-- ======================
-- Step 1: Base data
-- ======================
WITH base AS (
  SELECT DISTINCT
    `Restaurant Name`               AS restaurant_name,
    `Street Number`                 AS street_number,
    `Street Name`                   AS street_name,
    `Street Direction`              AS street_direction,
    `Street Type`                   AS street_type,
    `Street Unit`                   AS street_unit,
    `Street Address`                AS street_address,
    `Zip Code`                      AS zip,

    `Inspection Date`               AS inspection_date,
    `Inspection Type`               AS inspection_type,
    `Inspection Score`              AS inspection_score,
    `Inspection Year`               AS inspection_year,
    `Inspection Month`              AS inspection_month,

    -- 25 violation fields
    `Violation Description - 1`     AS violation_desc_1,
    `Violation Description - 2`     AS violation_desc_2,
    `Violation Description - 3`     AS violation_desc_3,
    `Violation Description - 4`     AS violation_desc_4,
    `Violation Description - 5`     AS violation_desc_5,
    `Violation Description - 6`     AS violation_desc_6,
    `Violation Description - 7`     AS violation_desc_7,
    `Violation Description - 8`     AS violation_desc_8,
    `Violation Description - 9`     AS violation_desc_9,
    `Violation Description - 10`    AS violation_desc_10,
    `Violation Description - 11`    AS violation_desc_11,
    `Violation Description - 12`    AS violation_desc_12,
    `Violation Description - 13`    AS violation_desc_13,
    `Violation Description - 14`    AS violation_desc_14,
    `Violation Description - 15`    AS violation_desc_15,
    `Violation Description - 16`    AS violation_desc_16,
    `Violation Description - 17`    AS violation_desc_17,
    `Violation Description - 18`    AS violation_desc_18,
    `Violation Description - 19`    AS violation_desc_19,
    `Violation Description - 20`    AS violation_desc_20,
    `Violation Description - 21`    AS violation_desc_21,
    `Violation Description - 22`    AS violation_desc_22,
    `Violation Description - 23`    AS violation_desc_23,
    `Violation Description - 24`    AS violation_desc_24,
    `Violation Description - 25`    AS violation_desc_25,

    `Lat Long Location`             AS lat_long_location,
    Has_Critical                    AS has_critical_flag_original,
    Latitude                        AS latitude,
    Longitude                       AS longitude,
    FileName                        AS filename,
    load_dt,
    source_file_path,
    source_file_name

  FROM LIVE.bronze_dallas_food_inspections
),

-- ======================
-- Step 2: Derived fields
-- ======================
derived AS (
  SELECT
    *,
    -- Count non-null violation description fields
    (
      CASE WHEN violation_desc_1  IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_2  IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_3  IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_4  IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_5  IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_6  IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_7  IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_8  IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_9  IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_10 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_11 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_12 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_13 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_14 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_15 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_16 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_17 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_18 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_19 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_20 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_21 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_22 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_23 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_24 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN violation_desc_25 IS NOT NULL THEN 1 ELSE 0 END
    ) AS violation_count,

    -- Any critical / urgent violations?
    (
      (violation_desc_1  RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_2  RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_3  RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_4  RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_5  RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_6  RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_7  RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_8  RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_9  RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_10 RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_11 RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_12 RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_13 RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_14 RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_15 RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_16 RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_17 RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_18 RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_19 RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_20 RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_21 RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_22 RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_23 RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_24 RLIKE '(?i)URGENT|CRITICAL') OR
      (violation_desc_25 RLIKE '(?i)URGENT|CRITICAL')
    ) AS has_critical_any

  FROM base
)

-- =============================================
-- FINAL FILTER: Score >= 90 must have <= 3 violations
-- =============================================
SELECT
  *
FROM derived
WHERE NOT (inspection_score >= 90 AND violation_count > 3);