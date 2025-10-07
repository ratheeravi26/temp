# Databricks notebook: 51_silver_dim_aad_group  (🟦 UPDATED)
# MAGIC %run ./00_config_and_common

from pyspark.sql import functions as F
from delta.tables import DeltaTable

src = spark.table("bronze.graph_groups_delta")

# filter non-removed
src = src.filter(F.col("removed") != True)\
    .withColumn("valid_from", F.current_timestamp())\
    .selectExpr(
        "group_id as aad_group_id",
        "display_name",
        "mail",
        "security_enabled",
        "mail_enabled",
        "group_types",
        "description",
        "valid_from"
    )

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.dim_aad_group (
  aad_group_id STRING,
  display_name STRING,
  mail STRING,
  security_enabled BOOLEAN,
  mail_enabled BOOLEAN,
  group_types ARRAY<STRING>,
  description STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN
) USING delta
""")

tgt = DeltaTable.forName(spark, "silver.dim_aad_group")

(tgt.alias("t")
 .merge(src.alias("s"), "t.aad_group_id = s.aad_group_id AND t.is_current = true")
 .whenMatchedUpdate(
     condition="t.display_name <> s.display_name OR t.mail <> s.mail OR t.security_enabled <> s.security_enabled",
     set={"valid_to": F.current_timestamp(), "is_current": F.lit(False)}
 )
 .whenNotMatchedInsert(values={
     "aad_group_id": "s.aad_group_id",
     "display_name": "s.display_name",
     "mail": "s.mail",
     "security_enabled": "s.security_enabled",
     "mail_enabled": "s.mail_enabled",
     "group_types": "s.group_types",
     "description": "s.description",
     "valid_from": "s.valid_from",
     "valid_to": F.lit(None).cast("timestamp"),
     "is_current": F.lit(True)
 }).execute())
