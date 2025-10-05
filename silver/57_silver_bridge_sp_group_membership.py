# 57_silver_bridge_sp_group_membership
# MAGIC %run ./00_config_and_common
from pyspark.sql import functions as F
from delta.tables import DeltaTable

src = (spark.table(f"{BRONZE}.mgdc_sharepoint_groups_v1")
        .withColumn("valid_from", F.col("ingest_ts"))
        .selectExpr(
            "get_json_object(_raw_data,'$.groupId') as sp_group_id",
            "get_json_object(_raw_data,'$.userId') as user_object_id",
            "valid_from",
            "source_file as source_run_id"
        ))

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.bridge_sp_group_membership (
  sp_group_id STRING,
  user_object_id STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN,
  source_run_id STRING
) USING delta
""")

target = DeltaTable.forName(spark, f"{CATALOG}.silver.bridge_sp_group_membership")
(target.alias("t")
 .merge(src.alias("s"),
        "t.sp_group_id = s.sp_group_id AND t.user_object_id = s.user_object_id AND t.is_current = true")
 .whenMatchedUpdate(set={"valid_to": F.current_timestamp(), "is_current": F.lit(False)})
 .whenNotMatchedInsert(values={
     "sp_group_id": "s.sp_group_id",
     "user_object_id": "s.user_object_id",
     "valid_from": "s.valid_from",
     "valid_to": F.lit(None).cast("timestamp"),
     "is_current": F.lit(True),
     "source_run_id": "s.source_run_id"
 }).execute()
)
