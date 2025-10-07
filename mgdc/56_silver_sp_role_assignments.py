# Databricks notebook: 56_silver_sp_role_assignments (🥈 SILVER)
# MAGIC %run ./00_config_and_common

from pyspark.sql import functions as F

src = (spark.table("bronze.mgdc_sharepoint_permissions_v1")
        .selectExpr(
            "ResourceId as resource_id",
            "ResourceType as resource_type",
            "SiteId as site_id",
            "SharedWithPrincipalId as principal_id",
            "SharedWithTypeV2 as shared_with_type_v2",
            "SharedWithLoginName as shared_with_login_name",
            "RoleDefinitionId as role_definition_id",
            "IsInherited as is_inherited"
        )
        .withColumn("valid_from", F.current_timestamp())
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True)))

src.write.mode("overwrite").saveAsTable("silver.sp_role_assignments")
