# Databricks notebook: 54_silver_bridge_aad_group_owners  (🆕 NEW)
# MAGIC %run ./00_config_and_common

from pyspark.sql import functions as F
from delta.tables import DeltaTable

src = spark.table("bronze.graph_groups_owners_delta")

src = (src
       .withColumn("valid_from", F.current_timestamp())
       .withColumn("is_current", F.when(F.col("action")=="add", F.lit(True)).otherwise(F.lit(False)))
       .selectExpr(
            "group_id as aad_group_id",
            "owner_id as owner_object_id",
            "valid_from",
            "null as valid_to",
            "is_current",
            "action"
       ))

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.bridge_aad_group_owners (
  aad_group_id STRING,
  owner_object_id STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN,
  action STRING
) USING delta
""")

tgt = DeltaTable.forName(spark, "silver.bridge_aad_group_owners")

(tgt.alias("t")
 .merge(src.alias("s"),
        "t.aad_group_id = s.aad_group_id AND t.owner_object_id = s.owner_object_id AND t.is_current = true")
 .whenMatchedUpdate(
     condition="s.action = 'remove'",
     set={"valid_to": F.current_timestamp(), "is_current": F.lit(False)}
 )
 .whenNotMatchedInsert(values={
     "aad_group_id": "s.aad_group_id",
     "owner_object_id": "s.owner_object_id",
     "valid_from": "s.valid_from",
     "valid_to": F.lit(None).cast("timestamp"),
     "is_current": F.col("s.is_current"),
     "action": "s.action"
 }).execute())
