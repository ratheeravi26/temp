# Databricks notebook: 10_bronze_mgdc_ingest (🟫 BRONZE)
# MAGIC %run ./00_config_and_common

from pyspark.sql import functions as F
import datetime as dt

# widgets
dbutils.widgets.text("dataset","")      # e.g. sharepoint_sites_v1, group_details_v0
dbutils.widgets.text("process_date","") # e.g. 2025-09-28
DATASET = dbutils.widgets.get("dataset")
PROCESS_DATE = dbutils.widgets.get("process_date") or dt.datetime.utcnow().strftime("%Y-%m-%d")

year, month, day = PROCESS_DATE.split("-")
BRONZE = "bronze"
RAW_BASE = "abfss://raw@<storageaccount>.dfs.core.windows.net/mgdc"

src_path = f"{RAW_BASE}/{DATASET}/year={year}/month={month}/day={day}/"
target_table = f"{BRONZE}.mgdc_{DATASET}"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target_table} (
  _raw_json STRING,
  year INT,
  month INT,
  day INT,
  ingest_ts TIMESTAMP,
  source_file STRING
) USING delta PARTITIONED BY (year, month, day)
""")

df = (spark.read.json(src_path)
      .withColumn("_raw_json", F.to_json(F.struct([F.col(c) for c in spark.read.json(src_path).columns])))
      .withColumn("year", F.lit(int(year)))
      .withColumn("month", F.lit(int(month)))
      .withColumn("day", F.lit(int(day)))
      .withColumn("ingest_ts", F.current_timestamp())
      .withColumn("source_file", F.input_file_name()))

(df.write
   .mode("overwrite")
   .option("replaceWhere", f"year={year} AND month={month} AND day={day}")
   .saveAsTable(target_table))

print(f"Loaded {df.count()} records for {DATASET} into {target_table}")
