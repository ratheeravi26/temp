# Databricks notebook: 50_silver_dim_user
# MAGIC %run ./00_config_and_common

from pyspark.sql import functions as F

src = (spark.table(f"{BRONZE}.graph_users_delta")
          .withColumn("valid_from", F.col("received_ts"))
          .selectExpr(
              "id as user_object_id",
              "user_principal_name as user_upn",
              "get_json_object(raw_json, '$.displayName') as display_name",
              "mail",
              "cast(account_enabled as boolean) as account_enabled",
              "valid_from",
              "delta_batch_id as source_run_id"
          ))

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.dim_user (
  user_object_id STRING,
  user_upn STRING,
  display_name STRING,
  mail STRING,
  account_enabled BOOLEAN,
  department STRING,
  job_title STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN,
  source_run_id STRING
)
USING delta
""")

from delta.tables import DeltaTable
target = DeltaTable.forName(spark, f"{CATALOG}.silver.dim_user")

(
  target.alias("t")
  .merge(
      src.alias("s"),
      "t.user_object_id = s.user_object_id AND t.is_current = true"
  )
  .whenMatchedUpdate(
      condition="""t.user_upn <> s.user_upn OR t.display_name <> s.display_name 
                   OR t.mail <> s.mail OR t.account_enabled <> s.account_enabled""",
      set={
        "valid_to": F.current_timestamp(),
        "is_current": F.lit(False)
      }
  )
  .whenNotMatchedInsert(
      values={
        "user_object_id": "s.user_object_id",
        "user_upn": "s.user_upn",
        "display_name": "s.display_name",
        "mail": "s.mail",
        "account_enabled": "s.account_enabled",
        "valid_from": "s.valid_from",
        "valid_to": F.lit(None).cast("timestamp"),
        "is_current": F.lit(True),
        "source_run_id": "s.source_run_id"
      }
  ).execute()
)
