-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE silver_chicago_food_inspections (

  -- Restaurant name cannot be null
  CONSTRAINT dba_name_not_null
    EXPECT (dba_name IS NOT NULL) ON VIOLATION DROP ROW,

  -- Inspection date / type cannot be null
  CONSTRAINT inspection_date_not_null
    EXPECT (Inspection_Date IS NOT NULL) ON VIOLATION DROP ROW,

  CONSTRAINT inspection_type_not_null
    EXPECT (Inspection_Type IS NOT NULL) ON VIOLATION DROP ROW,

  -- Zip cannot be null and must be valid as per your precomputed flag
  CONSTRAINT zip_not_null
    EXPECT (Zip IS NOT NULL) ON VIOLATION DROP ROW,

  CONSTRAINT zip_valid_flag
    EXPECT (Valid_Zip = 'Valid') ON VIOLATION DROP ROW,

  -- Results cannot be null
  CONSTRAINT results_not_null
    EXPECT (Results IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES (
  "quality" = "silver"
)
AS
WITH base AS (
  SELECT DISTINCT
    -- keep the original inspection ID
    Inspection_ID,

    -- use cleaned names when available
    COALESCE(`New_DBA Name`,  DBA_Name)  AS dba_name,
    COALESCE(`New_AKA Name`,  AKA_Name)  AS aka_name,
    COALESCE(`New_Address`,   Address)   AS address,
    COALESCE(`New_City`,      City)      AS city,

    License,
    Facility_Name,
    Risk,
    State,

    -- keep Zip and also a string version for easier use later
    Zip,
    CAST(Zip AS STRING)                 AS zip_str,

    Inspection_Date,
    Inspection_Type,
    Results,
    Violations,

    Latitude,
    Longitude,
    Location,
    FileName,

    Violation_Count,
    Valid_Coordinates,
    Valid_Zip,

    Violation_Code,
    Violation_Code2,
    Violation_Description,

    load_dt,
    source_file_path,
    source_file_name
  FROM workspace.default.bronze_chicago_food_inspections
)

SELECT
  *,
  CASE
    WHEN UPPER(Results) = 'PASS' THEN 90
    WHEN UPPER(Results) = 'PASS W/ CONDITIONS'
         OR UPPER(Results) = 'PASS W/ CONDITION'
         OR UPPER(Results) = 'PASS W/CONDITIONS'
      THEN 80
    WHEN UPPER(Results) = 'FAIL' THEN 70
    WHEN UPPER(Results) = 'NO ENTRY' THEN 0
    ELSE NULL
  END AS violation_score_chicago
FROM base;
