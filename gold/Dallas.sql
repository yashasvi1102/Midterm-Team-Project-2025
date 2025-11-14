-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE dim_dallas_date
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
FROM workspace.default.silver_dallas_food_inspections;


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_dallas_inspection_type
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  ROW_NUMBER() OVER (ORDER BY inspection_type) AS inspection_type_sk,
  inspection_type                              AS inspection_type_name
FROM (
  SELECT DISTINCT inspection_type
  FROM workspace.default.silver_dallas_food_inspections
);


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_dallas_restaurant
TBLPROPERTIES ("quality" = "gold")
AS
SELECT DISTINCT
  SHA2(CONCAT(
    COALESCE(restaurant_name, 'NA'), '||',
    COALESCE(street_address, 'NA'), '||',
    COALESCE(zip, 'NA')
  ), 256)          AS restaurant_key,

  restaurant_name,
  street_number,
  street_name,
  street_direction,
  street_type,
  street_unit,
  street_address,
  zip,
  latitude,
  longitude
FROM workspace.default.silver_dallas_food_inspections;


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_dallas_violation
TBLPROPERTIES ("quality" = "gold")
AS
SELECT DISTINCT
  SHA2(violation_desc, 256) AS violation_key,
  violation_desc
FROM (
  SELECT explode(array(
    violation_desc_1, violation_desc_2, violation_desc_3, violation_desc_4, violation_desc_5,
    violation_desc_6, violation_desc_7, violation_desc_8, violation_desc_9, violation_desc_10,
    violation_desc_11, violation_desc_12, violation_desc_13, violation_desc_14, violation_desc_15,
    violation_desc_16, violation_desc_17, violation_desc_18, violation_desc_19, violation_desc_20,
    violation_desc_21, violation_desc_22, violation_desc_23, violation_desc_24, violation_desc_25
  )) AS violation_desc
  FROM workspace.default.silver_dallas_food_inspections
) v
WHERE violation_desc IS NOT NULL;


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fact_dallas_inspection
TBLPROPERTIES ("quality" = "gold")
AS
-- 1) Take raw fields from SILVER
WITH base AS (
  SELECT
    restaurant_name,
    street_address,
    zip,
    CAST(inspection_date AS DATE) AS inspection_date,
    inspection_year,
    inspection_month,
    inspection_type,
    inspection_score,
    violation_count,
    has_critical_any,
    filename           AS source_file_name,
    load_dt            AS load_timestamp
  FROM workspace.default.silver_dallas_food_inspections
),

-- 2) Join to restaurant dimension to get restaurant_key
with_restaurant AS (
  SELECT
    b.*,
    r.restaurant_key
  FROM base b
  JOIN workspace.default.dim_dallas_restaurant r
    ON  b.restaurant_name = r.restaurant_name
    AND b.street_address  = r.street_address
    AND b.zip             = r.zip
),

-- 3) Join to date dimension to get date_key
with_date AS (
  SELECT
    wr.*,
    d.date_key
  FROM with_restaurant wr
  JOIN workspace.default.dim_dallas_date d
    ON wr.inspection_date = d.full_date
),

-- 4) Join to inspection_type dimension to get inspection_type_sk
with_type AS (
  SELECT
    wd.*,
    it.inspection_type_sk
  FROM with_date wd
  JOIN workspace.default.dim_dallas_inspection_type it
    ON wd.inspection_type = it.inspection_type_name
)

-- 5) Final fact table
SELECT
  -- surrogate fact key
  SHA2(CONCAT(
    restaurant_key, '|',
    CAST(date_key AS STRING), '|',
    COALESCE(inspection_type, 'NA')
  ), 256) AS inspection_fact_key,

  restaurant_key,
  date_key,
  inspection_year,
  inspection_month,
  inspection_type_sk,
  inspection_score,
  violation_count,
  has_critical_any,
  source_file_name,
  load_timestamp
FROM with_type;


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fact_dallas_inspection_violation
TBLPROPERTIES ("quality" = "gold")
AS
WITH exploded AS (
  SELECT
    fi.inspection_fact_key,
    explode(array(
      s.violation_desc_1, s.violation_desc_2, s.violation_desc_3, s.violation_desc_4, s.violation_desc_5,
      s.violation_desc_6, s.violation_desc_7, s.violation_desc_8, s.violation_desc_9, s.violation_desc_10,
      s.violation_desc_11, s.violation_desc_12, s.violation_desc_13, s.violation_desc_14, s.violation_desc_15,
      s.violation_desc_16, s.violation_desc_17, s.violation_desc_18, s.violation_desc_19, s.violation_desc_20,
      s.violation_desc_21, s.violation_desc_22, s.violation_desc_23, s.violation_desc_24, s.violation_desc_25
    )) AS violation_desc
  FROM workspace.default.silver_dallas_food_inspections s
  JOIN workspace.default.fact_dallas_inspection fi
    ON SHA2(CONCAT(
         COALESCE(s.restaurant_name, 'NA'), '||',
         COALESCE(s.street_address, 'NA'), '||',
         COALESCE(s.zip, 'NA')
       ), 256) = fi.restaurant_key
     AND YEAR(s.inspection_date) = fi.inspection_year
     AND MONTH(s.inspection_date) = fi.inspection_month
)
SELECT
  SHA2(CONCAT(
    e.inspection_fact_key, '||',
    dv.violation_key
  ), 256)                AS inspection_violation_key,
  e.inspection_fact_key,
  dv.violation_key,
  e.violation_desc
FROM exploded e
JOIN workspace.default.dim_dallas_violation dv
  ON e.violation_desc = dv.violation_desc