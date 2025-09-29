# Databricks notebook: 10_bronze_mgdc_autoloader
# MAGIC %run ./00_config_and_common

from pyspark.sql.functions import input_file_name, current_timestamp, lit

def autoload_parquet(src_path: str, target_table: str):
    (spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "parquet")
         .option("cloudFiles.includeExistingFiles", "true")
         .load(src_path)
         .withColumn("source_file", input_file_name())
         .withColumn("ingest_ts", current_timestamp())
    ).writeStream \
     .option("checkpointLocation", f"{src_path}/_checkpoint") \
     .trigger(availableNow=True) \
     .toTable(target_table)

# Create tables if not present (Delta will materialize on first write)
targets = {
    f"{BRONZE}.mgdc_sharepoint_sites_v1":       MGDC_PATHS["sharepoint_sites_v1"],
    f"{BRONZE}.mgdc_sharepoint_permissions_v1": MGDC_PATHS["sharepoint_permissions_v1"],
    f"{BRONZE}.mgdc_sharepoint_groups_v1":      MGDC_PATHS["sharepoint_groups_v1"],
    f"{BRONZE}.mgdc_group_details_v0":          MGDC_PATHS["group_details_v0"],
    f"{BRONZE}.mgdc_group_members_v0":          MGDC_PATHS["group_members_v0"],
    f"{BRONZE}.mgdc_group_owners_v0":           MGDC_PATHS["group_owners_v0"],
}

run_id = new_run_id("mgdc_bronze")
start = dt.datetime.utcnow()
try:
    for table, path in targets.items():
        print(f"Ingesting {path} -> {table}")
        autoload_parquet(path, table)
    log_run(run_id, "MGDC", "all", "SUCCESS", notes="Autoloader availableNow batch", started_at=start)
except Exception as e:
    log_run(run_id, "MGDC", "all", "FAILED", notes=str(e), started_at=start)
    raise
