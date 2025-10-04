# Databricks notebook: 10_bronze_mgdc_batch_ingest
# MAGIC %run ./00_config_and_common

from pyspark.sql import functions as F
import datetime as dt

# --- parameters ---
dbutils.widgets.text("dataset", "")    # e.g. sharepoint_sites_v1
dbutils.widgets.text("process_date", "")  # e.g. 2025-09-28 (YYYY-MM-DD)

DATASET   = dbutils.widgets.get("dataset")
PROCESS_DATE = dbutils.widgets.get("process_date")

if not PROCESS_DATE:
    PROCESS_DATE = dt.datetime.utcnow().strftime("%Y-%m-%d")

process_dt = dt.datetime.strptime(PROCESS_DATE, "%Y-%m-%d")
year, month, day = process_dt.year, process_dt.month, process_dt.day

print(f"Ingesting dataset={DATASET}, partition={year}/{month:02d}/{day:02d}")

# --- dataset paths ---
paths = {
    "sharepoint_sites_v1":      f"{RAW_BASE}/mgdc/sharepoint/sites_v1",
    "sharepoint_permissions_v1":f"{RAW_BASE}/mgdc/sharepoint/permissions_v1",
    "sharepoint_groups_v1":     f"{RAW_BASE}/mgdc/sharepoint/groups_v1",
    "group_details_v0":         f"{RAW_BASE}/mgdc/aad/group_details_v0",
    "group_members_v0":         f"{RAW_BASE}/mgdc/aad/group_members_v0",
    "group_owners_v0":          f"{RAW_BASE}/mgdc/aad/group_owners_v0",
}

src_path = paths[DATASET] + f"/year={year}/month={month:02d}/day={day:02d}/"

target_table = f"bronze.mgdc_{DATASET}"

# --- create target table if missing ---
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target_table} (
  _raw_data STRING,
  year INT,
  month INT,
  day INT,
  ingest_ts TIMESTAMP,
  source_file STRING
) USING delta
PARTITIONED BY (year, month, day)
""")

# --- read raw JSON ---
df = (spark.read.json(src_path)
        .withColumn("_raw_data", F.to_json(F.struct([F.col(c) for c in spark.read.json(src_path).columns])))
        .withColumn("year", F.lit(year))
        .withColumn("month", F.lit(month))
        .withColumn("day", F.lit(day))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name()))

# --- idempotent write ---
# overwrite only this partition, not the whole table
(df.write
   .mode("overwrite")
   .option("replaceWhere", f"year = {year} AND month = {month} AND day = {day}")
   .saveAsTable(target_table))

print(f"Ingested {df.count()} rows into {target_table} for {PROCESS_DATE}")
