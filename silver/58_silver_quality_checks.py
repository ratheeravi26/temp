# 58_silver_quality_checks
# MAGIC %run ./00_config_and_common
from pyspark.sql import functions as F

def dq_count(query: str) -> int:
    return spark.sql(query).count()

issues = []

if dq_count(f"SELECT * FROM {CATALOG}.silver.dim_user WHERE user_object_id IS NULL") > 0:
    issues.append("Null user_object_id in dim_user")

if dq_count(f"SELECT * FROM {CATALOG}.silver.dim_aad_group WHERE aad_group_id IS NULL") > 0:
    issues.append("Null aad_group_id in dim_aad_group")

if issues:
    raise ValueError("Data quality checks failed: " + "; ".join(issues))
else:
    print("All silver-layer checks passed.")
