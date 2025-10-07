# Databricks notebook: 55_silver_dim_sp_site (🥈 SILVER)
# MAGIC %run ./00_config_and_common

from pyspark.sql import functions as F

src = (spark.table("bronze.mgdc_sharepoint_sites_v1")
        .selectExpr("SiteId as site_id", "Url as url", "Title as title",
                    "ParentSiteId as parent_site_id")
        .withColumn("valid_from", F.current_timestamp())
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True)))

src.write.mode("overwrite").saveAsTable("silver.dim_sp_site")
