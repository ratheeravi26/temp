# Databricks Notebook: 02_GroupDetails_Silver
# Reads raw GroupDetails_v0 JSON from Bronze, transforms it, and writes to a Silver Delta table.

# Ensure 00_Setup_And_Configuration notebook has been run using %run./00_Setup_And_Configuration
# %run./00_Setup_And_Configuration

# --- Configuration ---
dataset_name = "GroupDetails_v0"
bronze_dataset_path = f"{bronze_base_path}{dataset_name}/" # Path to the dataset in Bronze
silver_table_name = "Silver_GroupDetails" # Changed name to avoid conflict with potential Gold Dim
silver_table_path = f"{silver_base_path}{silver_table_name}"

# Define schema for GroupDetails_v0 (refer to Section 4.1.2 of the report)
# This should match the structure of your JSON files.
group_details_schema = StructType()), True),
    StructField("pObjectId", StringType(), True), 
    StructField("ptenant", StringType(), True),
    StructField("SnapshotDate", StringType(), True) # Added by ADF or inferred from path, expected YYYY-MM-DD
])

# --- Read from Bronze --- 
# Uses the helper function from 00_Setup_And_Configuration
latest_partition_fs_path, latest_snapshot_date_from_path = get_latest_partition_path(bronze_dataset_path, spark, dbutils)

if not latest_partition_fs_path:
    print(f"No data found in Bronze for {dataset_name} at path {bronze_dataset_path}. Exiting.")
    dbutils.notebook.exit(f"No data in Bronze for {dataset_name}")

print(f"Reading {dataset_name} from Bronze path: {latest_partition_fs_path} (Snapshot Date from Path: {latest_snapshot_date_from_path})")
try:
    # It's crucial that the SnapshotDate field within the JSON matches latest_snapshot_date_from_path
    # or that latest_snapshot_date_from_path is used as the definitive SnapshotDate for this load.
    bronze_df_raw = spark.read.format("json").schema(group_details_schema).load(latest_partition_fs_path)
    
    # Validate if SnapshotDate from file content matches path, or use path date as authoritative
    # For this example, we'll assume the path-derived date is the ingestion date for this batch.
    # If the JSON files have their own "SnapshotDate" field, ensure it's handled consistently.
    # Here, we cast the SnapshotDate field from the schema (which might be from file content)
    # and also have latest_snapshot_date_from_path.
    
    # If the JSON files themselves contain a "SnapshotDate" field, use it. Otherwise, use the one from the path.
    if "SnapshotDate" not in bronze_df_raw.columns:
        bronze_df_raw = bronze_df_raw.withColumn("SnapshotDate", lit(latest_snapshot_date_from_path))
        print(f"Used SnapshotDate from path: {latest_snapshot_date_from_path}")
    else:
        print("Used SnapshotDate from JSON file content.")

except Exception as e:
    print(f"Error reading from Bronze for {dataset_name}: {e}")
    dbutils.notebook.exit(f"Failed to read Bronze data for {dataset_name}")

if bronze_df_raw.count() == 0:
    print(f"No records found in {latest_partition_fs_path} for {dataset_name}. Exiting.")
    dbutils.notebook.exit(f"No records to process for {dataset_name}")
    
# --- Transformations ---
transformed_df = bronze_df_raw \
 .withColumn("CreatedTimestamp", to_timestamp(col("createdDateTime"))) \
 .withColumn("DeletedTimestamp", to_timestamp(col("deletedDateTime"))) \
 .withColumn("GroupEmail", lower(trim(col("mail")))) \
 .withColumn("IsActive", col("deletedDateTime").isNull()) \
 .withColumn("PrimaryGroupType", 
                when(expr("array_contains(groupTypes, 'Unified')"), "Microsoft 365")
             .when(col("securityEnabled") == True, "Security") 
             .otherwise("Other")) \
 .withColumn("SensitivityLabel", 
                when(col("assignedLabels").isNotNull() & (expr("size(assignedLabels)") > 0), col("assignedLabels")["displayName"]) # Taking the first label's display name
             .otherwise(None)) \
 .withColumn("SnapshotDateCasted", to_date(col("SnapshotDate"), "yyyy-MM-dd")) \
 .withColumn("ingestion_timestamp", current_timestamp())

# Select final columns for Silver
silver_df = transformed_df.select(
    col("id").alias("GroupId_AAD"),
    col("displayName").alias("GroupDisplayName"),
    col("groupTypes").alias("GroupTypesRaw"),
    col("GroupEmail"),
    col("mailEnabled").alias("IsMailEnabled"),
    col("securityEnabled").alias("IsSecurityEnabled"),
    col("CreatedTimestamp"),
    col("DeletedTimestamp"),
    col("description").alias("GroupDescription"),
    col("visibility").alias("GroupVisibility"),
    col("onPremisesSyncEnabled").alias("IsOnPremisesSyncEnabled"),
    col("resourceProvisioningOptions").alias("ResourceProvisioningOptionsRaw"),
    col("SensitivityLabel"),
    col("IsActive"),
    col("PrimaryGroupType"),
    col("ptenant").alias("TenantId"),
    col("SnapshotDateCasted").alias("SnapshotDate"), 
    col("ingestion_timestamp")
).filter(col("GroupId_AAD").isNotNull()) # Ensure primary key is present

# --- Write to Silver (Delta Lake) ---
# This Silver table will be overwritten with the latest snapshot's processed data.
# This simplifies the Silver layer; SCD Type 2 complexity is handled when moving to Gold.
try:
    # We are processing one snapshot at a time. Overwrite the specific partition or the whole table if not partitioned.
    # If partitioning Silver by SnapshotDate:
    silver_df.write.format("delta").mode("overwrite") \
       .option("replaceWhere", f"SnapshotDate = '{latest_snapshot_date_from_path}'") \
       .option("overwriteSchema", "true") \
       .save(silver_table_path)
    
    # If the table doesn't exist yet and you want to create it with partitioning:
    if not DeltaTable.isDeltaTable(spark, silver_table_path):
       silver_df.write.format("delta").mode("overwrite").partitionBy("SnapshotDate").save(silver_table_path)
       spark.sql(f"CREATE TABLE IF NOT EXISTS {silver_table_name} USING DELTA LOCATION '{silver_table_path}' PARTITIONED BY (SnapshotDate)")
       print(f"Created new Silver table: {silver_table_name} at {silver_table_path}")

    print(f"Successfully wrote data to Silver table: {silver_table_name} at {silver_table_path} for snapshot {latest_snapshot_date_from_path}")

except Exception as e:
    print(f"Error writing to Silver for {dataset_name}: {e}")
    raise e
    
print(f"Finished processing {dataset_name} to Silver.")