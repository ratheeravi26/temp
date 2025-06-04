# Databricks Notebook: 00_Setup_And_Configuration

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, current_timestamp, lit, when, explode, lower, trim, to_timestamp, date_format, year, month, dayofmonth, from_json, get_json_object, regexp_replace, to_date, max as _max, min as _min, sum as _sum, avg, concat_ws, md5
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, ArrayType, LongType, DateType, DoubleType
from delta.tables import DeltaTable
import dbutils # Import dbutils

# Initialize Spark Session
spark = SparkSession.builder.appName("M365_Entitlements_ETL").getOrCreate()

# Define base paths for ADLS Gen2
adls_base_path = "abfss://your-container@your-storage-account.dfs.core.windows.net/" # Replace with your ADLS path
bronze_base_path = f"{adls_base_path}bronze/mgdc/"
silver_base_path = f"{adls_base_path}silver/m365_entitlements/"
gold_base_path = f"{adls_base_path}gold/m365_entitlements/"

# Helper function to get the path of the latest daily partition (year=YYYY/mnth=MM/day=DD)
def get_latest_partition_path(base_path, spark_session, dbutils_ref):
    """
    Finds the latest daily partition path (year=YYYY/mnth=MM/day=DD) within a base path.
    Returns the full path to the latest day's data and the snapshot date string (YYYY-MM-DD).
    """
    try:
        years = sorted([f.name for f in dbutils_ref.fs.ls(base_path) if f.name.startswith("year=")], reverse=True)
        if not years: return None, None
        latest_year_folder = years # e.g., "year=2023/"
        
        months_path = base_path + latest_year_folder
        months = sorted([f.name for f in dbutils_ref.fs.ls(months_path) if f.name.startswith("mnth=")], reverse=True)
        if not months: return None, None
        latest_month_folder = months # e.g., "mnth=12/"

        days_path = months_path + latest_month_folder
        days = sorted([f.name for f in dbutils_ref.fs.ls(days_path) if f.name.startswith("day=")], reverse=True)
        if not days: return None, None
        latest_day_folder = days # e.g., "day=31/"
        
        # Extract date from path components
        year_val = latest_year_folder.split('=')[-1].replace('/', '')
        month_val = latest_month_folder.split('=')[-1].replace('/', '')
        day_val = latest_day_folder.split('=')[-1].replace('/', '')
        latest_snapshot_date_from_path = f"{year_val}-{month_val.zfill(2)}-{day_val.zfill(2)}" # Ensure MM and DD are two digits
        
        return days_path + latest_day_folder, latest_snapshot_date_from_path
    except Exception as e:
        # Check if it's a FileNotFoundError specifically for the base_path
        if "java.io.FileNotFoundException" in str(e) and base_path in str(e):
            print(f"Base path not found for {base_path}: {e}")
            return None, None # Expected if dataset folder doesn't exist yet
        print(f"Error finding latest partition for {base_path}: {e}")
        return None, None

# Define a helper function for SCD Type 2 Merge (Attribute Changes)
def implement_scd2_merge(spark_session, source_df_with_current_snapshot_date, target_table_path, primary_key_cols, tracked_cols):
    """
    Implements SCD Type 2 merge logic for a Delta table for attribute changes.
    source_df_with_current_snapshot_date: DataFrame containing the current state of records. Must include 'SnapshotDate' column (cast to DateType) for effective/end dating.
    target_table_path: Path to the target Delta table.
    primary_key_cols: List of column names that form the primary key.
    tracked_cols: List of column names whose changes should trigger a new version.
    Target table must have: primary_key_cols, tracked_cols, effective_date, end_date, is_current.
    """
    current_processing_date_row = source_df_with_current_snapshot_date.select(_max(col("SnapshotDate")).alias("max_snapshot_date")).first()
    if not current_processing_date_row or not current_processing_date_row["max_snapshot_date"]:
        print("Error: SnapshotDate is missing or null in source_df for SCD2 merge. Cannot determine current processing date.")
        # dbutils.notebook.exit("SnapshotDate missing in source for SCD2") # Use this in Databricks
        raise ValueError("SnapshotDate missing in source_df_with_current_snapshot_date for SCD2 merge.")

    current_processing_date = current_processing_date_row["max_snapshot_date"]
    print(f"SCD2 Merge: Current processing date (from source SnapshotDate) is {current_processing_date}")

    # Create a hash of tracked columns to detect changes
    source_df_hashed = source_df_with_current_snapshot_date.withColumn("row_hash", md5(concat_ws("||", *[col(c).cast("string") for c in tracked_cols])))
    
    if not DeltaTable.isDeltaTable(spark_session, target_table_path):
        print(f"Target table {target_table_path} does not exist or is not a Delta table. Creating it.")
        insert_df = source_df_hashed.select(*primary_key_cols, *tracked_cols, "SnapshotDate") \
                             .withColumn("effective_date", col("SnapshotDate")) \
                             .withColumn("end_date", lit("9999-12-31").cast(DateType())) \
                             .withColumn("is_current", lit(True))
        insert_df.drop("SnapshotDate").write.format("delta").mode("overwrite").save(target_table_path)
        print(f"Target table {target_table_path} created and initial data loaded.")
        return

    target_delta_table = DeltaTable.forPath(spark_session, target_table_path)
    target_current_df = target_delta_table.toDF().filter(col("is_current") == True) \
                                     .withColumn("row_hash", md5(concat_ws("||", *[col(c).cast("string") for c in tracked_cols])))

    join_expr_str = " AND ".join([f"source.{pk} = target.{pk}" for pk in primary_key_cols])

    # Records that have changed (UPDATE old, INSERT new)
    changed_records_source_df = source_df_hashed.alias("source") \
     .join(target_current_df.alias("target"), expr(join_expr_str)) \
     .where(col("source.row_hash")!= col("target.row_hash")) \
     .select("source.*")

    # New records (INSERT new)
    new_records_source_df = source_df_hashed.alias("source") \
     .join(target_current_df.alias("target"), expr(join_expr_str), "left_anti") \
     .select("source.*")

    # Records that are no longer in source but are current in target (Soft Delete/End Date)
    deleted_records_target_df = target_current_df.alias("target") \
      .join(source_df_hashed.alias("source"), expr(join_expr_str), "left_anti") \
      .select("target.*") # Selects columns from target to identify records to expire

    # Stage 1: Expire old records for those that have changed or were deleted
    # Expiry date is the day before the current processing date (SnapshotDate from source)
    expiry_date = current_processing_date - expr("INTERVAL 1 DAY")

    staged_updates_for_expiry = changed_records_source_df.select(*primary_key_cols) \
                             .unionByName(deleted_records_target_df.select(*primary_key_cols)) \
                             .distinct() # Ensure we only try to expire each PK once

    if staged_updates_for_expiry.count() > 0:
        update_condition_expire = " AND ".join([f"target_delta.{pk} = updates.{pk}" for pk in primary_key_cols])
        
        target_delta_table.alias("target_delta").merge(
            staged_updates_for_expiry.alias("updates"),
            expr(update_condition_expire + " AND target_delta.is_current = true")
        ).whenMatchedUpdate(set = {
            "end_date": expiry_date,
            "is_current": lit(False)
        }).execute()
        print(f"Expired {staged_updates_for_expiry.count()} old/deleted records in {target_table_path}.")

    # Stage 2: Insert new versions of changed records and brand new records
    # Effective date for these new records is the current processing date (SnapshotDate from source)
    insert_df = changed_records_source_df.select(*primary_key_cols, *tracked_cols, "SnapshotDate") \
             .unionByName(new_records_source_df.select(*primary_key_cols, *tracked_cols, "SnapshotDate")) \
             .withColumn("effective_date", col("SnapshotDate")) \
             .withColumn("end_date", lit("9999-12-31").cast(DateType())) \
             .withColumn("is_current", lit(True)) \
             .distinct() # Avoids re-inserting if source had duplicates for same PK and SnapshotDate
    
    if insert_df.count() > 0:
        # Ensure no attempt to insert a record that would violate PK + effective_date uniqueness if target already has it
        # This is more of a safeguard; the logic above should handle distinct states.
        target_delta_table.alias("target").merge(
            insert_df.alias("source"),
            expr(join_expr_str + " AND target.effective_date = source.effective_date") # Check if this exact version already exists
        ).whenNotMatchedInsertAll().execute() # Only insert if truly new for this effective_date
        
        print(f"Processed {insert_df.count()} new/updated current records for {target_table_path}.")
    
    if staged_updates_for_expiry.count() == 0 and insert_df.count() == 0:
        print(f"No changes detected for {target_table_path} based on current source snapshot.")

print("Setup and Configuration Notebook Executed.")
print(f"Bronze layer base path: {bronze_base_path}")
print(f"Silver layer base path: {silver_base_path}")
print(f"Gold layer base path: {gold_base_path}")

# Example usage of get_latest_partition_path (can be commented out)
# test_path, test_date = get_latest_partition_path(bronze_base_path + "GroupDetails_v0/", spark, dbutils)
# print(f"Test latest path: {test_path}, Test latest date: {test_date}")