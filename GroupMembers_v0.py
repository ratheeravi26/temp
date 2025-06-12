# Databricks Notebook: Bronze Ingestion for GroupMembers_v0
# Purpose: Ingests raw GroupMembers_v0 data from ADLS Gen2 into a partitioned Bronze Delta table.
# Assumes: Raw JSONL files are landed in ADLS daily under a path like:
# {input_base_path}/year=YYYY/month=MM/day=DD/

# COMMAND ----------
# DBTITLE 1,Widgets for Parameterization
dbutils.widgets.text("input_base_path", "/mnt/adls_raw/m365/groupmembers_v0", "Base Input Path for GroupMembers_v0")
dbutils.widgets.text("bronze_database_name", "m365_bronze", "Bronze Layer Database Name")
dbutils.widgets.text("bronze_table_name", "groupmembers_v0", "Bronze Layer Table Name for GroupMembers")
dbutils.widgets.text("snapshot_date_str", "YYYY-MM-DD", "Snapshot Date (YYYY-MM-DD)")

# COMMAND ----------
# DBTITLE 2,Retrieve Widget Values
input_base_path = dbutils.widgets.get("input_base_path")
bronze_database_name = dbutils.widgets.get("bronze_database_name")
bronze_table_name = dbutils.widgets.get("bronze_table_name")
snapshot_date_str = dbutils.widgets.get("snapshot_date_str")

full_bronze_table_name = f"{bronze_database_name}.{bronze_table_name}"

# COMMAND ----------
# DBTITLE 3,Import Libraries
from pyspark.sql.functions import col, lit, current_timestamp, to_date, year, month, dayofmonth, input_file_name
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime

# COMMAND ----------
# DBTITLE 4,Helper Functions and Initial Setup
def get_logger():
    def log(message):
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}")
    return log

logger = get_logger()

logger.info(f"Starting Bronze ingestion for {full_bronze_table_name} for snapshot date: {snapshot_date_str}")

try:
    snapshot_dt = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
except ValueError:
    logger.error("Invalid snapshot_date_str format. Please use YYYY-MM-DD.")
    dbutils.notebook.exit("Invalid snapshot_date_str format.")

year_str = snapshot_dt.strftime("%Y")
month_str = snapshot_dt.strftime("%m")
day_str = snapshot_dt.strftime("%d")

input_path = f"{input_base_path}/year={year_str}/month={month_str}/day={day_str}/"
logger.info(f"Constructed input path: {input_path}")

# COMMAND ----------
# DBTITLE 5,Define Source Schema for GroupMembers_v0
# Based on Microsoft Graph Data Connect documentation for GroupMembers_v0
# Ref: https://github.com/microsoftgraph/dataconnect-solutions/blob/main/Datasets/data-connect-dataset-groupmembers.md
group_members_schema = StructType()

# COMMAND ----------
# DBTITLE 6,Read Raw Data from ADLS
try:
    logger.info(f"Reading raw JSON data from: {input_path}")
    try:
        dbutils.fs.ls(input_path)
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            logger.warning(f"Input path {input_path} not found. Skipping processing for this date.")
            dbutils.notebook.exit(f"Input path {input_path} not found.")
        else:
            raise e

    raw_df = spark.read.format("json") \
       .schema(group_members_schema) \
       .load(input_path)
    
    if raw_df.rdd.isEmpty():
        logger.warning(f"No data found in {input_path} or data is not in expected JSON format. Skipping processing for this date.")
        dbutils.notebook.exit(f"No data found in {input_path}.")

except Exception as e:
    logger.error(f"Error reading raw data from {input_path}: {str(e)}")
    raise

# COMMAND ----------
# DBTITLE 7,Add Bronze Layer Metadata and Partition Columns
logger.info("Adding metadata columns (ingestion_timestamp, source_file_name, snapshot_date, snapshot_year, snapshot_month, snapshot_day)")
bronze_df = raw_df \
   .withColumn("ingestion_timestamp", current_timestamp()) \
   .withColumn("source_file_name", input_file_name()) \
   .withColumn("snapshot_date", to_date(lit(snapshot_date_str), "yyyy-MM-dd")) \
   .withColumn("snapshot_year", year(col("snapshot_date"))) \
   .withColumn("snapshot_month", month(col("snapshot_date"))) \
   .withColumn("snapshot_day", dayofmonth(col("snapshot_date")))

# COMMAND ----------
# DBTITLE 8,Write to Bronze Delta Table
try:
    logger.info(f"Writing data to Bronze table: {full_bronze_table_name}")
    
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {bronze_database_name}")
    logger.info(f"Ensured database {bronze_database_name} exists.")

    (bronze_df.write
       .format("delta")
       .mode("overwrite")
       .option("replaceWhere", f"snapshot_date = '{snapshot_date_str}'")
       .partitionBy("snapshot_year", "snapshot_month", "snapshot_day")
       .option("mergeSchema", "true")
       .saveAsTable(full_bronze_table_name))
    
    logger.info(f"Successfully wrote data to {full_bronze_table_name} for snapshot_date {snapshot_date_str}")

except Exception as e:
    logger.error(f"Error writing data to {full_bronze_table_name}: {str(e)}")
    raise

# COMMAND ----------
# DBTITLE 9,Post-Write Operations (Optional)
# Similar to GroupDetails, OPTIMIZE and VACUUM can be considered.
logger.info(f"Bronze ingestion for {full_bronze_table_name} completed for snapshot date: {snapshot_date_str}")

# COMMAND ----------
dbutils.notebook.exit("Successfully completed GroupMembers_v0 Bronze ingestion.")