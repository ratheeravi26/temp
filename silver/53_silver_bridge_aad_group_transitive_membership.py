# 53_silver_bridge_aad_group_transitive_membership
# MAGIC %run ./00_config_and_common
from pyspark.sql import functions as F
from delta.tables import DeltaTable

edges = spark.table(f"{CATALOG}.silver.bridge_aad_group_membership") \
             .where("is_current = true") \
             .select("aad_group_id", "member_object_id", "member_type")

# iterative BFS to resolve nested groups to users
users = edges.filter("member_type = 'User'") \
             .selectExpr("aad_group_id", "member_object_id as user_object_id", "array(aad_group_id) as via_group_path")

groups = edges.filter("member_type = 'Group'") \
              .selectExpr("aad_group_id as parent", "member_object_id as child")

frontier = groups
while True:
    next_hop = (frontier.alias("f")
                .join(groups.alias("g"), F.col("f.child") == F.col("g.parent"), "inner")
                .select(F.col("f.parent").alias("parent"), F.col("g.child").alias("child"))
                .distinct())
    new_edges = next_hop.exceptAll(frontier)
    if new_edges.count() == 0:
        break
    frontier = frontier.union(new_edges).distinct()

deep_users = (frontier.alias("f")
              .join(edges.filter("member_type = 'User'").alias("u"),
                    F.col("f.child") == F.col("u.aad_group_id"))
              .select(F.col("f.parent").alias("aad_group_id"),
                      F.col("u.member_object_id").alias("user_object_id"))
              .distinct()
              .withColumn("via_group_path", F.array(F.col("aad_group_id"), F.col("user_object_id"))))

all_users = users.unionByName(deep_users).distinct() \
                 .withColumn("valid_from", F.current_timestamp()) \
                 .withColumn("source_run_id", F.lit("transitive_calc"))

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.bridge_aad_group_transitive_membership (
  aad_group_id STRING,
  user_object_id STRING,
  via_group_path ARRAY<STRING>,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN,
  source_run_id STRING
) USING delta
""")

target = DeltaTable.forName(spark, f"{CATALOG}.silver.bridge_aad_group_transitive_membership")
(target.alias("t")
 .merge(all_users.alias("s"),
        "t.aad_group_id = s.aad_group_id AND t.user_object_id = s.user_object_id AND t.is_current = true")
 .whenMatchedUpdate(set={"valid_to": F.current_timestamp(), "is_current": F.lit(False)})
 .whenNotMatchedInsert(values={
     "aad_group_id": "s.aad_group_id",
     "user_object_id": "s.user_object_id",
     "via_group_path": "s.via_group_path",
     "valid_from": "s.valid_from",
     "valid_to": F.lit(None).cast("timestamp"),
     "is_current": F.lit(True),
     "source_run_id": "s.source_run_id"
 }).execute()
)
