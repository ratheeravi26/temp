# Databricks notebook: 10_bronze_mgdc_autoloader_json
# MAGIC %run ./00_config_and_common

from pyspark.sql.functions import input_file_name, current_timestamp, col

def autoload_json(src_path: str, target_table: str):
    (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.includeExistingFiles", "true")
            .load(src_path)
            # the path contains year=YYYY/month=MM/day=DD; Autoloader will automatically
            # extract these into columns `year`, `month`, `day`
            .withColumn("source_file", input_file_name())
            .withColumn("ingest_ts", current_timestamp())
    ).writeStream \
     .option("checkpointLocation", f"{src_path}/_checkpoint") \
     .trigger(availableNow=True) \
     .partitionBy("year","month","day") \
     .toTable(target_table)

# dataset -> raw path
targets = {
    f"{BRONZE}.mgdc_sharepoint_sites_v1":
        f"{RAW_BASE}/mgdc/sharepoint/sites_v1/",
    f"{BRONZE}.mgdc_sharepoint_permissions_v1":
        f"{RAW_BASE}/mgdc/sharepoint/permissions_v1/",
    f"{BRONZE}.mgdc_sharepoint_groups_v1":
        f"{RAW_BASE}/mgdc/sharepoint/groups_v1/",
    f"{BRONZE}.mgdc_group_details_v0":
        f"{RAW_BASE}/mgdc/aad/group_details_v0/",
    f"{BRONZE}.mgdc_group_members_v0":
        f"{RAW_BASE}/mgdc/aad/group_members_v0/",
    f"{BRONZE}.mgdc_group_owners_v0":
        f"{RAW_BASE}/mgdc/aad/group_owners_v0/"
}

run_id = new_run_id("mgdc_json_bronze")
start = dt.datetime.utcnow()
try:
    for table, path in targets.items():
        print(f"Ingesting {path} -> {table}")
        autoload_json(path, table)
    log_run(run_id, "MGDC", "all-json", "SUCCESS", notes="JSON Autoloader with year/month/day partitions", started_at=start)
except Exception as e:
    log_run(run_id, "MGDC", "all-json", "FAILED", notes=str(e), started_at=start)
    raise
