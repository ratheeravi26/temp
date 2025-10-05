# 54_silver_dim_sp_site
# MAGIC %run ./00_config_and_common
from pyspark.sql import functions as F
from delta.tables import DeltaTable

src = (spark.table(f"{BRONZE}.mgdc_sharepoint_sites_v1")
          .withColumn("valid_from", F.col("ingest_ts"))
          .selectExpr(
              "get_json_object(_raw_data, '$.id') as site_id",
              "get_json_object(_raw_data, '$.webUrl') as url",
              "get_json_object(_raw_data, '$.name') as title",
              "get_json_object(_raw_data, '$.parentSiteId') as parent_site_id",
              "valid_from",
              "source_file as source_run_id"
          ))

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.dim_sp_site (
  site_id STRING,
  url STRING,
  title STRING,
  parent_site_id STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN,
  source_run_id STRING
) USING delta
""")

target = DeltaTable.forName(spark, f"{CATALOG}.silver.dim_sp_site")
(target.alias("t")
 .merge(src.alias("s"), "t.site_id = s.site_id AND t.is_current = true")
 .whenMatchedUpdate(
     condition="t.url <> s.url OR t.title <> s.title",
     set={"valid_to": F.current_timestamp(), "is_current": F.lit(False)}
 )
 .whenNotMatchedInsert(values={
     "site_id": "s.site_id",
     "url": "s.url",
     "title": "s.title",
     "parent_site_id": "s.parent_site_id",
     "valid_from": "s.valid_from",
     "valid_to": F.lit(None).cast("timestamp"),
     "is_current": F.lit(True),
     "source_run_id": "s.source_run_id"
 }).execute()
)
