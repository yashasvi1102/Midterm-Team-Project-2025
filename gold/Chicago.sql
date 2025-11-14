-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE dim_chicago_date
TBLPROPERTIES ("quality" = "gold")
AS
SELECT DISTINCT
  CAST(date_format(inspection_date, 'yyyyMMdd') AS INT) AS date_key,
  CAST(inspection_date AS DATE)                    AS full_date,
  YEAR(inspection_date)                           AS year,
  MONTH(inspection_date)                          AS month,
  DATE_FORMAT(inspection_date, 'MMMM')            AS month_name,
  QUARTER(inspection_date)                        AS quarter,
  WEEKOFYEAR(inspection_date)                     AS week_of_year,
  DAY(inspection_date)                            AS day_of_month
FROM workspace.default.silver_chicago_food_inspections;


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_chicago_inspection_type
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  ROW_NUMBER() OVER (ORDER BY inspection_type) AS inspection_type_sk,
  inspection_type                              AS inspection_type_name,
  CASE
    WHEN inspection_type LIKE '%Complaint%' THEN 'Complaint'
    WHEN inspection_type LIKE '%Canvass%'   THEN 'Canvass'
    WHEN inspection_type LIKE '%License%'   THEN 'License'
    ELSE 'Other'
  END                                          AS inspection_category
FROM (
  SELECT DISTINCT inspection_type
  FROM workspace.default.silver_chicago_food_inspections
);


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_chicago_restaurant
TBLPROPERTIES ("quality" = "gold")
AS
SELECT DISTINCT
  SHA2(CONCAT(
    COALESCE(License, 'NA'), '||',
    COALESCE(dba_name, 'NA'), '||',
    COALESCE(Address, 'NA'), '||',
    COALESCE(Zip, 'NA')
  ), 256)                AS restaurant_key,

  License         AS license_number,
  dba_name,
  aka_name,
  Facility_Name   AS facility_name,
  risk            AS risk_category,
  Address         AS address,
  city,
  state,
  Zip             AS zip_code,
  Latitude        AS latitude,
  Longitude       AS longitude
FROM workspace.default.silver_chicago_food_inspections;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_chicago_violation
TBLPROPERTIES ("quality" = "gold")
AS
SELECT DISTINCT
  SHA2(violation_code, 256)                     AS violation_key,
  violation_code,
  violation_description,
  CASE
    WHEN violation_description RLIKE '(?i)critical|priority' THEN 'Critical'
    WHEN violation_description RLIKE '(?i)serious'           THEN 'Serious'
    ELSE 'Minor'
  END                                           AS severity_level
FROM workspace.default.silver_chicago_food_inspections
WHERE violation_code IS NOT NULL;


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fact_chicago_inspection
TBLPROPERTIES ("quality" = "gold")
AS
-- 1) Take raw fields from SILVER
WITH base AS (
  SELECT
    inspection_id,
    License,
    dba_name,
    Address,
    Zip,
    CAST(inspection_date AS DATE) AS inspection_date,
    inspection_type,
    results        AS inspection_result,
    risk           AS risk_category,
    load_dt        AS load_timestamp
  FROM workspace.default.silver_chicago_food_inspections
),

-- 2) Join to restaurant dimension to get restaurant_key
with_restaurant AS (
  SELECT
    b.*,
    r.restaurant_key
  FROM base b
  JOIN workspace.default.dim_chicago_restaurant r
    ON  b.License = r.license_number
    AND b.Address = r.address
    AND b.Zip     = r.zip_code
),

-- 3) Join to date dimension to get date_key
with_date AS (
  SELECT
    wr.*,
    d.date_key
  FROM with_restaurant wr
  JOIN workspace.default.dim_chicago_date d
    ON wr.inspection_date = d.full_date
),

-- 4) Join to inspection_type dimension to get inspection_type_sk
with_type AS (
  SELECT
    wd.*,
    it.inspection_type_sk
  FROM with_date wd
  JOIN workspace.default.dim_chicago_inspection_type it
    ON wd.inspection_type = it.inspection_type_name
)

-- 5) Final fact table
SELECT
  SHA2(CONCAT(inspection_id, '|', CAST(date_key AS STRING)), 256)
    AS inspection_fact_key,

  restaurant_key,
  date_key,
  inspection_id,
  inspection_date,
  inspection_type_sk,
  inspection_result,
  risk_category,
  Zip     AS zip_code,
  Address AS address,
  load_timestamp
FROM with_type;


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fact_chicago_inspection_violation
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  -- surrogate key
  SHA2(CONCAT(
    fi.inspection_fact_key, '||',
    dv.violation_key
  ), 256)                 AS inspection_violation_key,

  fi.inspection_fact_key,
  dv.violation_key,
  s.violation_code,
  s.violation_description
FROM workspace.default.silver_chicago_food_inspections s
JOIN workspace.default.fact_chicago_inspection fi
  ON s.inspection_id = fi.inspection_id
JOIN workspace.default.dim_chicago_violation dv
  ON s.violation_code = dv.violation_code;
