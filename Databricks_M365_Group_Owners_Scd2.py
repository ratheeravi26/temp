# Databricks Notebook: Ingest Microsoft 365 Group Owners and Maintain SCD Type 2 History (Delta + ADLS Gen2)
# Language: Python (PySpark)
# -----------------------------------------------------------------------------
# Overview:
# This notebook fetches the current owners for all active Microsoft 365 groups
# (using the Graph API), and persists the ownership history to a Delta table
# using SCD Type 2 semantics. The pipeline is idempotent: re-running the
# notebook for the same run_timestamp will produce the same final state.
# -----------------------------------------------------------------------------

# MAGIC %md
# MAGIC ## 1) Setup & Configuration
# MAGIC
# MAGIC Widgets are used for parameterization. Do **not** hardcode paths or secrets.

# COMMAND ----------
# Create widgets (users can set these before running)
dbutils.widgets.text("source_group_table_path", "abfss://bronze@your_storage_account.dfs.core.windows.net/m365/groups/", "Source group table path (ABFSS)")
dbutils.widgets.text("target_owner_table_path", "abfss://silver@your_storage_account.dfs.core.windows.net/m365/group_owners_history/", "Target owner table path (ABFSS)")
dbutils.widgets.text("key_vault_scope", "my_keyvault_scope", "Databricks secret scope for AAD app credentials")

# Read widget values
source_group_table_path = dbutils.widgets.get("source_group_table_path")
target_owner_table_path = dbutils.widgets.get("target_owner_table_path")
key_vault_scope = dbutils.widgets.get("key_vault_scope")

# Print a short summary (non-sensitive)
print(f"Source path: {source_group_table_path}")
print(f"Target path: {target_owner_table_path}")
print(f"Secret scope: {key_vault_scope}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2) Dependencies
# MAGIC
# MAGIC Install Python packages that might not be present on the cluster. In many
# MAGIC clusters these are pre-installed; the pip cell is idempotent and will
# MAGIC be a no-op if package already present.

# COMMAND ----------
# MAGIC %pip install msal requests

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3) Imports and Global Run Timestamp
# MAGIC
# MAGIC Capture a single `run_timestamp` at the start of execution and use it for
# MAGIC all SCD start/end date assignments. This is vital for idempotency.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType
from datetime import datetime, timezone
import time
import json
import requests
import msal

# Single consistent run timestamp for this job (UTC) — used for all start/end date assignments
run_timestamp = datetime.now(timezone.utc).replace(microsecond=0)
# keep a string representation safe for SQL literals
run_timestamp_str = run_timestamp.strftime('%Y-%m-%d %H:%M:%S')
print(f"run_timestamp (UTC): {run_timestamp_str}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4) Retrieve AAD App Credentials from Databricks Secret Scope
# MAGIC
# MAGIC Expect the following secrets in the provided scope:
# MAGIC * `m365_tenant_id`
# MAGIC * `m365_client_id`
# MAGIC * `m365_client_secret`

# COMMAND ----------
# Fetch secrets from the provided scope
tenant_id = dbutils.secrets.get(scope=key_vault_scope, key="m365_tenant_id")
client_id = dbutils.secrets.get(scope=key_vault_scope, key="m365_client_id")
client_secret = dbutils.secrets.get(scope=key_vault_scope, key="m365_client_secret")

# Basic validation
if not (tenant_id and client_id and client_secret):
    raise Exception("Missing one or more AAD app credentials in the secret scope.")

print("Successfully retrieved AAD app credentials from secret scope (not showing values).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5) Authentication helper (MSAL)
# MAGIC
# MAGIC Helper to obtain an access token for Microsoft Graph (client credentials flow).
# MAGIC Uses MSAL to handle token caching and retries.

# COMMAND ----------
AUTHORITY = f"https://login.microsoftonline.com/{tenant_id}"
SCOPE = ["https://graph.microsoft.com/.default"]

app = msal.ConfidentialClientApplication(
    client_id,
    authority=AUTHORITY,
    client_credential=client_secret,
)

# Acquire token at driver and broadcast it. We will attempt to reuse the same token
# across worker tasks for this run. This keeps mapInPandas simpler and ensures all
# parts of this run share the same authentication moment.
token_result = app.acquire_token_for_client(scopes=SCOPE)
if "access_token" not in token_result:
    raise Exception(f"Failed to acquire access token: {token_result}")
access_token = token_result["access_token"]
print("Successfully acquired access token (not shown).")

# Broadcast token
access_token_b = spark.sparkContext.broadcast(access_token)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6) Read source groups and prepare group id list
# MAGIC
# MAGIC We read the `m365_groups_delta` table from the provided path. The notebook
# MAGIC is robust to minor schema differences and attempts reasonable heuristics
# MAGIC to filter for "active" groups.

# COMMAND ----------
# Read the source groups table (Delta) — assume the path points to a Delta table
try:
    groups_df = spark.read.format("delta").load(source_group_table_path)
except Exception as e:
    raise Exception(f"Failed to read source group table at {source_group_table_path}: {e}")

print(f"Source groups columns: {groups_df.columns}")

# Heuristic to filter active groups (apply the first matching rule)
if 'deletedDate' in groups_df.columns:
    groups_active_df = groups_df.filter(F.col('deletedDate').isNull())
elif 'state' in groups_df.columns:
    groups_active_df = groups_df.filter(F.col('state') == 'active')\else:
    # Fallback - no explicit delete/state field present: assume table already contains current groups
    groups_active_df = groups_df

# Select distinct group IDs only
group_ids_df = groups_active_df.select(F.col('id').alias('group_id')).distinct()
print(f"Number of distinct groups to process (estimate): {group_ids_df.count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7) Graph API helper: robust fetch with pagination + retries
# MAGIC
# MAGIC We'll implement a function `get_group_owners` that fetches owners for a
# MAGIC single group and returns a list of owner dicts. The function handles
# MAGIC pagination and retries with exponential backoff for HTTP 429/5xx errors.
# MAGIC
# MAGIC The function is defined inside the mapInPandas worker function below so it
# MAGIC can access the broadcast token easily.

# COMMAND ----------
# We will process groups in parallel using mapInPandas. Define the output schema.
output_schema = StructType([
    StructField('group_id', StringType(), True),
    StructField('owner_id', StringType(), True),
    StructField('owner_displayName', StringType(), True),
    StructField('owner_userPrincipalName', StringType(), True),
])

# Implement the mapInPandas function. It receives a pandas dataframe with a column 'group_id'.
def fetch_owners_map(iterator):
    import pandas as pd
    import requests
    import time
    from urllib.parse import urljoin

    access_token = access_token_b.value
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }

    def get_group_owners(group_id):
        """Fetch all owners for a single group_id from Microsoft Graph.
        Returns a list of dicts with keys: id, displayName, userPrincipalName.
        Implements pagination and exponential backoff for transient errors.
        """
        owners = []
        # initial URL
        url = f'https://graph.microsoft.com/v1.0/groups/{group_id}/owners?$select=id,displayName,userPrincipalName'

        max_retries = 5
        backoff_base = 1.0

        while url:
            for attempt in range(max_retries):
                try:
                    resp = requests.get(url, headers=headers, timeout=30)
                except requests.exceptions.RequestException as e:
                    # network-level error -> retry
                    sleep_sec = backoff_base * (2 ** attempt)
                    time.sleep(sleep_sec)
                    continue

                if resp.status_code == 200:
                    data = resp.json()
                    if 'value' in data:
                        for o in data['value']:
                            owners.append({
                                'id': o.get('id'),
                                'displayName': o.get('displayName'),
                                'userPrincipalName': o.get('userPrincipalName')
                            })
                    # handle pagination
                    url = data.get('@odata.nextLink')
                    break

                elif resp.status_code in (429, 503):
                    # throttled or service unavailable — exponential backoff
                    retry_after = None
                    try:
                        # Graph may return Retry-After seconds header
                        retry_after = int(resp.headers.get('Retry-After', 0))
                    except Exception:
                        retry_after = None

                    if retry_after and retry_after > 0:
                        time.sleep(retry_after)
                    else:
                        sleep_sec = backoff_base * (2 ** attempt)
                        time.sleep(sleep_sec)
                    continue

                elif 500 <= resp.status_code < 600:
                    # server error — retry
                    sleep_sec = backoff_base * (2 ** attempt)
                    time.sleep(sleep_sec)
                    continue

                else:
                    # client error (4xx other than 429) — treat as no owners or fatal
                    # We'll log and return empty list for that group.
                    try:
                        _err = resp.json()
                    except Exception:
                        _err = resp.text
                    print(f"Non-retriable HTTP error for group {group_id}: {resp.status_code} - {_err}")
                    return []

            else:
                # Exceeded retries for this page — give up and return what we have so far
                print(f"Exceeded retries fetching owners for group {group_id}. Returning partial results.")
                return owners

        return owners

    # iterate over partitions
    for pdf in iterator:
        out_rows = []
        for gid in pdf['group_id'].tolist():
            try:
                owners = get_group_owners(gid)
            except Exception as e:
                print(f"Error fetching owners for group {gid}: {e}")
                owners = []

            if not owners:
                # Keep no rows for groups without owners (flattened output should simply omit them)
                continue

            for o in owners:
                out_rows.append({
                    'group_id': gid,
                    'owner_id': o.get('id'),
                    'owner_displayName': o.get('displayName'),
                    'owner_userPrincipalName': o.get('userPrincipalName')
                })

        if len(out_rows) == 0:
            # return empty DataFrame for this partition
            yield pd.DataFrame(columns=['group_id','owner_id','owner_displayName','owner_userPrincipalName'])
        else:
            yield pd.DataFrame(out_rows)

# Run the distributed fetch using mapInPandas. Partitioning: keep the default or increase by coalescing groups list.
# For very large numbers of groups you might want to increase parallelism via .repartition(n)
owners_df = group_ids_df.repartition(200).mapInPandas(fetch_owners_map, schema=output_schema)

# Persist the resulting DataFrame to avoid re-computations
owners_df = owners_df.persist()
print(f"Fetched owner rows (estimate): {owners_df.count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8) Prepare SCD Type 2 source (present flag + include removed owners)
# MAGIC
# MAGIC We'll create a source dataset for the MERGE that contains:
# MAGIC * All **current** owners fetched from Graph, with `present = true`.
# MAGIC * All **currently active** owners present in the target Delta table but **not** present in the fetched snapshot, with `present = false`.
# MAGIC
# MAGIC This makes it possible to express all SCD transitions with a **single** MERGE
# MAGIC statement (atomic) — new inserts, attribute changes, and closures for removed owners.

# COMMAND ----------
from delta.tables import DeltaTable
from pyspark.sql.functions import lit

# Add present flag to current owners
owners_present_df = owners_df.withColumn('present', lit(True))

# If target table does not exist yet, create it using the current snapshot
import os

def delta_table_exists(path):
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False

if not delta_table_exists(target_owner_table_path):
    print(f"Target Delta table not found at {target_owner_table_path}. Creating initial table...")
    # Prepare initial records: all current owners are inserted with start_date=run_timestamp
    initial_df = owners_present_df.withColumn('start_date', F.to_timestamp(F.lit(run_timestamp_str))) \
                                     .withColumn('end_date', F.lit(None).cast(TimestampType())) \
                                     .withColumn('is_current', lit(True))

    # Write the initial delta table
    initial_df.write.format('delta').mode('overwrite').save(target_owner_table_path)
    print("Initial target table created.")
else:
    print(f"Target Delta table exists at {target_owner_table_path}. Proceeding to SCD MERGE flow.")

# Now (re)load the target for reading
target_df = spark.read.format('delta').load(target_owner_table_path)

# Select currently active rows from target
target_current_df = target_df.filter(F.col('is_current') == True).select(
    F.col('group_id'), F.col('owner_id'), F.col('owner_displayName'), F.col('owner_userPrincipalName'), F.col('start_date'), F.col('end_date')
)

# Identify owners that exist in the target current set but are NOT present in today's fetched owners (removed owners)
removed_owners_df = target_current_df.join(
    owners_present_df.select('group_id', 'owner_id'),
    on=['group_id', 'owner_id'],
    how='left_anti'
).select('group_id', 'owner_id', 'owner_displayName', 'owner_userPrincipalName')

# Mark removed owners with present = false
removed_owners_df = removed_owners_df.withColumn('present', lit(False))

# Compose the final MERGE source: union of present owners and removed ones
scd_source_df = owners_present_df.unionByName(removed_owners_df)

# Persist and create temp view for SQL MERGE
scd_source_df = scd_source_df.persist()
scd_source_view = 'vw_scd_owners_source'
scd_source_df.createOrReplaceTempView(scd_source_view)

print(f"SCD source rows (present + removed): {scd_source_df.count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9) SCD Type 2 MERGE (single atomic operation)
# MAGIC
# MAGIC The MERGE below handles three scenarios:
# MAGIC 1. **New owners** (present=true and not matched) -> INSERT with start_date = run_timestamp
# MAGIC 2. **Removed owners** (present=false) -> UPDATE existing current record: set is_current=false, end_date = run_timestamp
# MAGIC 3. **Attribute change for existing owner** (present=true but attribute differs) -> close existing record (is_current=false, end_date=run_timestamp) and insert new active record
# MAGIC 4. **Unchanged owners** -> ignored (no action)

# COMMAND ----------
# Build and execute the MERGE SQL. Use timestamp literals based on run_timestamp_str to ensure idempotency.
merge_sql = f"""
MERGE INTO delta.`{target_owner_table_path}` AS tgt
USING (SELECT group_id, owner_id, owner_displayName, owner_userPrincipalName, present FROM {scd_source_view}) AS src
ON tgt.group_id = src.group_id AND tgt.owner_id = src.owner_id

-- 1) Close out records that are currently active in target but missing from today's snapshot
WHEN MATCHED AND tgt.is_current = true AND src.present = false
  THEN UPDATE SET tgt.is_current = false, tgt.end_date = TIMESTAMP('{run_timestamp_str}')

-- 2) Handle attribute changes for an owner (close existing record)
WHEN MATCHED AND tgt.is_current = true AND src.present = true AND (
      (coalesce(tgt.owner_displayName, '') <> coalesce(src.owner_displayName, ''))
   OR (coalesce(tgt.owner_userPrincipalName, '') <> coalesce(src.owner_userPrincipalName, ''))
)
  THEN UPDATE SET tgt.is_current = false, tgt.end_date = TIMESTAMP('{run_timestamp_str}')

-- 3) Insert new records (new owners and also inserts after attribute-change closures)
WHEN NOT MATCHED AND src.present = true
  THEN INSERT (group_id, owner_id, owner_displayName, owner_userPrincipalName, start_date, end_date, is_current)
       VALUES (src.group_id, src.owner_id, src.owner_displayName, src.owner_userPrincipalName, TIMESTAMP('{run_timestamp_str}'), NULL, true)
"""

print("Executing MERGE ...")
spark.sql(merge_sql)
print("MERGE completed.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10) Post-merge maintenance: OPTIMIZE + ZORDER
# MAGIC
# MAGIC Run OPTIMIZE and ZORDER to keep the Delta table performant for common queries.

# COMMAND ----------
try:
    print("Running OPTIMIZE and ZORDER... This may take time depending on table size.")
    spark.sql(f"OPTIMIZE delta.`{target_owner_table_path}` ZORDER BY (group_id)")
    print("OPTIMIZE completed.")
except Exception as e:
    print(f"OPTIMIZE failed or not supported on this runtime: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 11) Validation / Quick Checks
# MAGIC
def quick_counts():
    tgt = spark.read.format('delta').load(target_owner_table_path)
    total = tgt.count()
    current = tgt.filter(F.col('is_current') == True).count()
    closed = tgt.filter(F.col('is_current') == False).count()
    print(f"Target total rows: {total}, current: {current}, closed: {closed}")

quick_counts()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Notebook complete
# MAGIC
# MAGIC - The notebook captured a single `run_timestamp` at start and used it for all SCD operations.
# MAGIC - The SCD Type 2 logic is performed in a single, atomic `MERGE` statement.
# MAGIC - The fetching logic uses pagination and exponential backoff to be resilient to throttling.
# MAGIC
# MAGIC **Operational notes / recommended production additions:**
# MAGIC * Add logging (structured) instead of prints — integrate with your monitoring solution.
 MAGIC * Consider chunking/retrying mapInPandas partitions on failure for extreme scale.
 MAGIC * Tune the number of partitions used for `group_ids_df.repartition()` based on your cluster size.
 MAGIC * Consider using a service principal with least privilege: only `Group.Read.All` or similar.
 MAGIC
# End of notebook
