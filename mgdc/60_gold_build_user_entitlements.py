# Databricks notebook: 60_gold_build_user_entitlements (🥇 GOLD)
# MAGIC %run ./00_config_and_common

from pyspark.sql import functions as F

roles = spark.table("silver.sp_role_assignments")
spgroups = spark.table("silver.sharepoint_groups_v1")
spmembers = spark.table("silver.sharepoint_group_members_v1")
aadgroups = spark.table("silver.dim_aad_group")
aadmem = spark.table("silver.bridge_aad_group_transitive_membership")
users = spark.table("silver.dim_user")

# expand SharePoint groups
sp_expanded = (roles.filter("shared_with_type_v2='SharePointGroup'")
    .join(spmembers, roles.principal_id == spmembers.group_id, "inner")
    .join(users, spmembers.user_id == users.user_object_id, "inner")
    .select("site_id","resource_id","resource_type","role_definition_id","user_object_id")
    .withColumn("grant_type", F.lit("SharePointGroup"))
)

# expand AAD security groups
aad_expanded = (roles.filter("shared_with_type_v2='SecurityGroup'")
    .join(aadmem, roles.principal_id == aadmem.aad_group_id, "inner")
    .select("site_id","resource_id","resource_type","role_definition_id","user_object_id")
    .withColumn("grant_type", F.lit("SecurityGroup"))
)

# direct users
direct_users = (roles.filter("shared_with_type_v2 in ('InternalUser','B2BUser')")
    .select("site_id","resource_id","resource_type","role_definition_id",
            F.col("principal_id").alias("user_object_id"))
    .withColumn("grant_type", F.lit("Direct"))
)

entitlements = direct_users.unionByName(sp_expanded).unionByName(aad_expanded).dropDuplicates()

entitlements.write.mode("overwrite").saveAsTable("gold.sharepoint_user_entitlements")

print(f"Gold entitlements: {entitlements.count()}")
