# 52_silver_bridge_aad_group_membership
# MAGIC %run ./00_config_and_common
from pyspark.sql import functions as F
from delta.tables import DeltaTable

src = (spark.table(f"{BRONZE}.graph_group_members_delta")
          .withColumn("valid_from", F.col("received_ts"))
          .selectExpr(
              "group_id as aad_group_id",
              "principal_id as member_object_id",
              "principal_type as member_type",
              "valid_from",
              "delta_batch_id as source_run_id"
          ))

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.bridge_aad_group_membership (
  aad_group_id STRING,
  member_object_id STRING,
  member_type STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN,
  source_run_id STRING
) USING delta
""")

target = DeltaTable.forName(spark, f"{CATALOG}.silver.bridge_aad_group_membership")

(target.alias("t")
 .merge(src.alias("s"),
        "t.aad_group_id = s.aad_group_id AND t.member_object_id = s.member_object_id AND t.is_current = true")
 .whenMatchedUpdate(set={"valid_to": F.current_timestamp(), "is_current": F.lit(False)})
 .whenNotMatchedInsert(values={
     "aad_group_id": "s.aad_group_id",
     "member_object_id": "s.member_object_id",
     "member_type": "s.member_type",
     "valid_from": "s.valid_from",
     "valid_to": F.lit(None).cast("timestamp"),
     "is_current": F.lit(True),
     "source_run_id": "s.source_run_id"
 }).execute()
)
