-- Databricks notebook source
-- BRONZE: Chicago Food Inspections (DLT)
CREATE OR REFRESH STREAMING LIVE TABLE bronze_chicago_food_inspections
TBLPROPERTIES (
  "quality" = "bronze",
  "delta.columnMapping.mode" = "name"
)
AS
SELECT
  *,
  current_timestamp()        AS load_dt,
  _metadata.file_path        AS source_file_path,
  _metadata.file_name        AS source_file_name
FROM STREAM cloud_files(
  "/Volumes/workspace/damg7370/datastore/midterm_data/chicago/",
  "csv",
  map(
    "header", "true",
    "cloudFiles.inferColumnTypes", "true"
  )
);
