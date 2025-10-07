# Databricks notebook: 53_silver_bridge_aad_group_owners (🥈 SILVER)
# MAGIC %run ./00_config_and_common

from pyspark.sql import functions as F
from delta.tables import DeltaTable

src = (spark.table("bronze.mgdc_group_owners_v0")
        .selectExpr("GroupId as aad_group_id", "OwnerId as owner_object_id")
        .withColumn("valid_from", F.current_timestamp())
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True)))

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.bridge_aad_group_owners (
  aad_group_id STRING,
  owner_object_id STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN
) USING delta
""")

target = DeltaTable.forName(spark, "silver.bridge_aad_group_owners")

(target.alias("t")
 .merge(src.alias("s"),
        "t.aad_group_id = s.aad_group_id AND t.owner_object_id = s.owner_object_id AND t.is_current = true")
 .whenNotMatchedInsert(values={
     "aad_group_id": "s.aad_group_id",
     "owner_object_id": "s.owner_object_id",
     "valid_from": "s.valid_from",
     "valid_to": "s.valid_to",
     "is_current": "s.is_current"
 }).execute())
