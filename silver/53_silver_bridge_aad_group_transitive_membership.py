# Databricks notebook: 53_silver_bridge_aad_group_transitive_membership (🟦 UPDATED & COMPLETE)
# MAGIC %run ./00_config_and_common

from pyspark.sql import functions as F

# ---------- load inputs ----------
direct_mem = spark.table("silver.bridge_aad_group_membership").filter("is_current = true")
aad_groups = spark.table("silver.dim_aad_group").filter("is_current = true")
aad_users  = spark.table("silver.dim_user").filter("is_current = true")

# optional: include historical baseline once (for first run)
if spark.catalog.tableExists("silver.bridge_aad_group_membership_baseline"):
    base_mem = spark.table("silver.bridge_aad_group_membership_baseline")
    direct_mem = base_mem.unionByName(direct_mem, allowMissingColumns=True).dropDuplicates(["aad_group_id","member_object_id"])

# ---------- build transitive closure ----------
# 1. separate user vs nested group memberships
users_mem  = direct_mem.join(aad_users, direct_mem.member_object_id == aad_users.user_object_id, "inner")\
                       .select(direct_mem.aad_group_id, F.col("member_object_id").alias("user_object_id"))
nested_mem = direct_mem.join(aad_groups, direct_mem.member_object_id == aad_groups.aad_group_id, "inner")\
                       .select(F.col("aad_group_id").alias("parent_group_id"),
                               F.col("member_object_id").alias("child_group_id"))

# 2. iteratively expand nested groups until no new users found
expanded = users_mem
level = 0
new_links = nested_mem
while new_links.count() > 0:
    level += 1
    # join child group -> its user members
    child_users = (new_links
        .join(users_mem, new_links.child_group_id == users_mem.aad_group_id, "inner")
        .select(new_links.parent_group_id.alias("aad_group_id"), "user_object_id"))
    expanded = expanded.unionByName(child_users).dropDuplicates(["aad_group_id","user_object_id"])
    # find next nesting level
    new_links = (new_links
        .join(nested_mem, new_links.child_group_id == nested_mem.parent_group_id, "inner")
        .select(nested_mem.parent_group_id.alias("parent_group_id"), nested_mem.child_group_id.alias("child_group_id"))
        .dropDuplicates())

print(f"Transitive expansion completed, levels={level}")

# ---------- prepare final output ----------
result = (expanded
          .withColumn("valid_from", F.current_timestamp())
          .withColumn("valid_to", F.lit(None).cast("timestamp"))
          .withColumn("is_current", F.lit(True))
          .dropDuplicates(["aad_group_id","user_object_id"]))

spark.sql("""
CREATE TABLE IF NOT EXISTS silver.bridge_aad_group_transitive_membership (
  aad_group_id STRING,
  user_object_id STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN
) USING delta
""")

result.write.mode("overwrite").saveAsTable("silver.bridge_aad_group_transitive_membership")

print(f"Wrote {result.count()} transitive memberships to silver.bridge_aad_group_transitive_membership")
