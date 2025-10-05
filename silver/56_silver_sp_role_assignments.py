# 56_silver_sp_role_assignments
# MAGIC %run ./00_config_and_common
from pyspark.sql import functions as F
from delta.tables import DeltaTable

src = (spark.table(f"{BRONZE}.mgdc_sharepoint_permissions_v1")
        .withColumn("valid_from", F.col("ingest_ts"))
        .selectExpr(
            "get_json_object(_raw_data,'$.resourceId') as resource_id",
            "get_json_object(_raw_data,'$.resourceType') as resource_type",
            "get_json_object(_raw_data,'$.siteId') as site_id",
            "get_json_object(_raw_data,'$.principalType') as principal_type",
            "get_json_object(_raw_data,'$.principalId') as principal_id",
            "get_json_object(_raw_data,'$.roleDefinitionId') as role_definition_id",
            "cast(get_json_object(_raw_data,'$.isInherited') as boolean) as is_inherited",
            "get_json_object(_raw_data,'$.inheritanceBrokenAt') as inheritance_broken_at",
            "valid_from",
            "source_file as source_run_id"
        ))

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.sp_role_assignments (
  resource_id STRING,
  resource_type STRING,
  site_id STRING,
  principal_type STRING,
  principal_id STRING,
  role_definition_id STRING,
  is_inherited BOOLEAN,
  inheritance_broken_at STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN,
  source_run_id STRING
) USING delta
""")

target = DeltaTable.forName(spark, f"{CATALOG}.silver.sp_role_assignments")
(target.alias("t")
 .merge(src.alias("s"),
        "t.resource_id = s.resource_id AND t.principal_id = s.principal_id "
        "AND t.role_definition_id = s.role_definition_id AND t.is_current = true")
 .whenMatchedUpdate(set={"valid_to": F.current_timestamp(), "is_current": F.lit(False)})
 .whenNotMatchedInsert(values={
     "resource_id": "s.resource_id",
     "resource_type": "s.resource_type",
     "site_id": "s.site_id",
     "principal_type": "s.principal_type",
     "principal_id": "s.principal_id",
     "role_definition_id": "s.role_definition_id",
     "is_inherited": "s.is_inherited",
     "inheritance_broken_at": "s.inheritance_broken_at",
     "valid_from": "s.valid_from",
     "valid_to": F.lit(None).cast("timestamp"),
     "is_current": F.lit(True),
     "source_run_id": "s.source_run_id"
 }).execute()
)
