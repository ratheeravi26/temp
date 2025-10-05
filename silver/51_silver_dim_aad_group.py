# Databricks notebook: 51_silver_dim_aad_group
# MAGIC %run ./00_config_and_common
from pyspark.sql import functions as F
from delta.tables import DeltaTable

src = (spark.table(f"{BRONZE}.graph_groups_delta")
          .withColumn("valid_from", F.col("received_ts"))
          .selectExpr(
              "id as aad_group_id",
              "get_json_object(raw_json, '$.displayName') as display_name",
              "cast(get_json_object(raw_json,'$.securityEnabled') as boolean) as security_enabled",
              "cast(get_json_object(raw_json,'$.mailEnabled') as boolean) as mail_enabled",
              "get_json_object(raw_json,'$.mail') as mail",
              "from_json(get_json_object(raw_json,'$.groupTypes'),'array<string>') as group_types",
              "get_json_object(raw_json,'$.description') as description",
              "valid_from",
              "delta_batch_id as source_run_id"
          ))

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.dim_aad_group (
  aad_group_id STRING,
  display_name STRING,
  security_enabled BOOLEAN,
  mail_enabled BOOLEAN,
  mail STRING,
  group_types ARRAY<STRING>,
  description STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN,
  source_run_id STRING
) USING delta
""")

target = DeltaTable.forName(spark, f"{CATALOG}.silver.dim_aad_group")

(target.alias("t")
 .merge(src.alias("s"), "t.aad_group_id = s.aad_group_id AND t.is_current = true")
 .whenMatchedUpdate(
     condition="""t.display_name <> s.display_name
                  OR t.security_enabled <> s.security_enabled
                  OR t.mail_enabled <> s.mail_enabled
                  OR t.mail <> s.mail
                  OR t.group_types <> s.group_types
                  OR t.description <> s.description""",
     set={"valid_to": F.current_timestamp(), "is_current": F.lit(False)}
 )
 .whenNotMatchedInsert(
     values={
         "aad_group_id": "s.aad_group_id",
         "display_name": "s.display_name",
         "security_enabled": "s.security_enabled",
         "mail_enabled": "s.mail_enabled",
         "mail": "s.mail",
         "group_types": "s.group_types",
         "description": "s.description",
         "valid_from": "s.valid_from",
         "valid_to": F.lit(None).cast("timestamp"),
         "is_current": F.lit(True),
         "source_run_id": "s.source_run_id"
     }
 ).execute()
)
