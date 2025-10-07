# Databricks notebook: 51_silver_dim_aad_group (🥈 SILVER)
# MAGIC %run ./00_config_and_common

from pyspark.sql import functions as F
from delta.tables import DeltaTable

src = (spark.table("bronze.mgdc_group_details_v0")
        .selectExpr("Id as aad_group_id", "DisplayName as display_name",
                    "Mail as mail", "MailEnabled as mail_enabled",
                    "SecurityEnabled as security_enabled",
                    "GroupTypes as group_types",
                    "Description as description")
        .withColumn("valid_from", F.current_timestamp()))

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.dim_aad_group (
  aad_group_id STRING,
  display_name STRING,
  mail STRING,
  mail_enabled BOOLEAN,
  security_enabled BOOLEAN,
  group_types ARRAY<STRING>,
  description STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN
) USING delta
""")

target = DeltaTable.forName(spark, "silver.dim_aad_group")

(target.alias("t")
 .merge(src.alias("s"), "t.aad_group_id = s.aad_group_id AND t.is_current = true")
 .whenMatchedUpdate(
     condition="t.display_name <> s.display_name OR t.mail <> s.mail",
     set={"valid_to": F.current_timestamp(), "is_current": F.lit(False)}
 )
 .whenNotMatchedInsert(values={
     "aad_group_id": "s.aad_group_id",
     "display_name": "s.display_name",
     "mail": "s.mail",
     "mail_enabled": "s.mail_enabled",
     "security_enabled": "s.security_enabled",
     "group_types": "s.group_types",
     "description": "s.description",
     "valid_from": "s.valid_from",
     "valid_to": F.lit(None).cast("timestamp"),
     "is_current": F.lit(True)
 }).execute())
