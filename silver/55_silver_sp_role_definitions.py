# 55_silver_sp_role_definitions
# MAGIC %run ./00_config_and_common
from pyspark.sql import functions as F

src = (spark.table(f"{BRONZE}.mgdc_sharepoint_permissions_v1")
        .selectExpr(
            "get_json_object(_raw_data, '$.roleDefinitionId') as role_definition_id",
            "get_json_object(_raw_data, '$.roleName') as permission_label",
            "get_json_object(_raw_data, '$.basePermissions') as permissions",
            "get_json_object(_raw_data, '$.isCustomRole') as is_custom"
        ).distinct())

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.sp_role_definitions
USING delta AS SELECT * FROM VALUES (0) WHERE false
""")
src.write.mode("overwrite").insertInto(f"{CATALOG}.silver.sp_role_definitions")
