# Databricks Notebook: 02_GroupDetails_Gold
# Reads transformed GroupDetails from Silver and creates/updates Dim_Group_SCD2 in Gold.

# Ensure 00_Setup_And_Configuration notebook has been run using %run./00_Setup_And_Configuration
# %run./00_Setup_And_Configuration

# --- Configuration ---
silver_source_table_name = "Silver_GroupDetails" # Output of the previous Silver notebook
silver_source_table_path = f"{silver_base_path}{silver_source_table_name}"

gold_dim_table_name = "Dim_Group_SCD2"
gold_dim_table_path = f"{gold_base_path}{gold_dim_table_name}"

# --- Read from Silver ---
# We need the latest snapshot from the Silver table to process for SCD2.
try:
    silver_group_details_all_snapshots_df = spark.read.format("delta").load(silver_source_table_path)
    
    latest_snapshot_in_silver_row = silver_group_details_all_snapshots_df.select(_max(col("SnapshotDate")).alias("max_date")).first()
    
    if not latest_snapshot_in_silver_row or not latest_snapshot_in_silver_row["max_date"]:
        print(f"No data or no SnapshotDate found in Silver table {silver_source_table_name}. Exiting.")
        dbutils.notebook.exit("No Silver data to process for Gold.")
        
    latest_snapshot_in_silver = latest_snapshot_in_silver_row["max_date"]
    
    # This is the current state of all groups as of the latest snapshot in Silver
    source_for_scd_df = silver_group_details_all_snapshots_df.filter(col("SnapshotDate") == lit(latest_snapshot_in_silver))
    print(f"Read latest snapshot {latest_snapshot_in_silver} from {silver_source_table_name} for SCD2 processing.")

    if source_for_scd_df.count() == 0:
        print(f"No records found for snapshot {latest_snapshot_in_silver} in {silver_source_table_name}. Exiting.")
        dbutils.notebook.exit(f"No records in Silver for snapshot {latest_snapshot_in_silver}")

except Exception as e:
    print(f"Error reading Silver table {silver_source_table_name}: {e}")
    dbutils.notebook.exit(f"Failed to read Silver data for {silver_source_table_name}")

# --- Transformations for SCD Type 2 ---
# Select columns relevant for the dimension and for change tracking.
# Ensure SnapshotDate is present for the implement_scd2_merge helper.
scd_input_df = source_for_scd_df.select(
    col("GroupId_AAD"),
    col("GroupDisplayName"),
    col("GroupTypesRaw"), 
    col("GroupEmail"),
    col("IsMailEnabled"),
    col("IsSecurityEnabled"),
    col("CreatedTimestamp"),
    col("GroupDescription"),
    col("GroupVisibility"),
    col("IsOnPremisesSyncEnabled"),
    col("ResourceProvisioningOptionsRaw"),
    col("SensitivityLabel"),
    col("PrimaryGroupType"),
    col("TenantId"),
    col("IsActive"), 
    col("DeletedTimestamp"), 
    col("SnapshotDate") # Crucial for the SCD2 helper function, must be DateType
).distinct() # Ensure unique group states for the given snapshot

# Define primary key and tracked columns for SCD Type 2
scd_primary_key_cols =
scd_tracked_cols =

# --- Write to Gold (Dim_Group_SCD2) using the helper function ---
print(f"Starting SCD Type 2 merge for {gold_dim_table_name}...")
try:
    implement_scd2_merge(spark, scd_input_df, gold_dim_table_path, scd_primary_key_cols, scd_tracked_cols)
except ValueError as ve:
    print(f"ValueError during SCD2 merge: {ve}")
    dbutils.notebook.exit(f"SCD2 Merge failed due to ValueError: {ve}")
except Exception as e:
    print(f"An unexpected error occurred during SCD2 merge: {e}")
    raise e


# --- Optional: Optimize Gold Table ---
# Consider running OPTIMIZE and VACUUM periodically
# spark.sql(f"OPTIMIZE delta.`{gold_dim_table_path}` ZORDER BY (GroupId_AAD)")
# spark.sql(f"VACUUM delta.`{gold_dim_table_path}` RETAIN 168 HOURS")

print(f"Finished processing {silver_source_table_name} to Gold table {gold_dim_table_name}.")