# Databricks notebook: 54_silver_bridge_aad_group_transitive_membership (🥈 SILVER)
# MAGIC %run ./00_config_and_common

from pyspark.sql import functions as F

direct_mem = spark.table("silver.bridge_aad_group_membership")
aad_groups = spark.table("silver.dim_aad_group")
aad_users  = spark.table("silver.dim_user")

# Separate user vs group members
users_mem = direct_mem.join(aad_users, direct_mem.member_object_id == aad_users.user_object_id, "inner")\
                      .select(direct_mem.aad_group_id, "user_object_id")
nested_mem = direct_mem.join(aad_groups, direct_mem.member_object_id == aad_groups.aad_group_id, "inner")\
                      .select(F.col("aad_group_id").alias("parent_group_id"),
                              F.col("member_object_id").alias("child_group_id"))

expanded = users_mem
level = 0
new_links = nested_mem
while new_links.count() > 0:
    level += 1
    child_users = new_links.join(users_mem, new_links.child_group_id == users_mem.aad_group_id, "inner")\
                           .select(new_links.parent_group_id.alias("aad_group_id"), "user_object_id")
    expanded = expanded.unionByName(child_users).dropDuplicates(["aad_group_id","user_object_id"])
    new_links = new_links.join(nested_mem, new_links.child_group_id == nested_mem.parent_group_id, "inner")\
                         .select(nested_mem.parent_group_id.alias("parent_group_id"), nested_mem.child_group_id.alias("child_group_id")).dropDuplicates()

result = expanded.withColumn("valid_from", F.current_timestamp())\
                 .withColumn("valid_to", F.lit(None).cast("timestamp"))\
                 .withColumn("is_current", F.lit(True))

result.write.mode("overwrite").saveAsTable("silver.bridge_aad_group_transitive_membership")

print(f"Transitive memberships: {result.count()}")
