# ==============================================================================
#  Databricks Telemetry Issue Reporting Script
# ==============================================================================
# This script replicates and optimizes the logic from the provided SQL query
# for a production environment in Databricks using PySpark.
# ==============================================================================

# Import necessary libraries
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from datetime import datetime, timedelta

# This code assumes you are running in a Databricks notebook environment
# where a SparkSession is already available as 'spark'.

# ==============================================================================
# 1. CONFIGURATION & SETUP
# ==============================================================================
print("Starting configuration and setup...")

# --- JDBC Connection Details ---
# It's a critical security best practice to store sensitive information like
# credentials in Databricks secrets and retrieve them using dbutils.
# Example: jdbc_user = dbutils.secrets.get(scope="your_secret_scope", key="your_jdbc_user_key")

jdbc_hostname = "<your_sql_server_hostname>"
jdbc_port = 1433
jdbc_database = "<your_database_name>"
jdbc_user = "<your_username>"
jdbc_password = "<your_password>" # IMPORTANT: Replace with dbutils.secrets.get() in production

# Create the JDBC URL for connecting to SQL Server
jdbc_url = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};databaseName={jdbc_database}"

# Define connection properties, including the driver
connection_properties = {
    "user": jdbc_user,
    "password": jdbc_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# --- Helper Function to Read Data ---
def read_sql_table(table_name):
    """Reads a table from the configured SQL Server into a Spark DataFrame."""
    print(f"Reading table: {table_name}...")
    return spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

# --- Define Date Ranges for Reporting ---
now = datetime.now()
blast_30_days_start = now - timedelta(days=30)
blast_7_days_start = now - timedelta(days=7)
blast_3_days_start = now - timedelta(days=3)
blast_24_hours_start = now - timedelta(hours=24)

print("Configuration and date ranges set up successfully. 🚀")
print("-" * 50)

# ==============================================================================
# 2. LOAD SOURCE DATA & APPLY INITIAL FILTERS
# ==============================================================================
print("Loading source data from SQL Server...")

# Load all necessary tables
# Note: Provide the actual table names as they exist in your SQL database.
df_gpin = read_sql_table("gpin")
df_employee = read_sql_table("Employee")
df_org_unit = read_sql_table("OrganizationalUnit")
df_work_location = read_sql_table("WorkLocation")
df_telemetry_raw = read_sql_table("MyHubRemoteAnaylics")
df_thin_clients = read_sql_table("ThinClients") # Assuming a direct table name

# --- Create applicableEmployees DataFrame (Filters the user base) ---
df_applicable_employees = df_gpin.join(df_employee, df_gpin.HriEmployeeId == df_employee.Id, "inner") \
    .join(df_org_unit, df_employee.OrganizationalUnitId == df_org_unit.Id, "inner") \
    .join(df_work_location, df_gpin.workLocationId == df_work_location.Id, "inner") \
    .filter(
        (F.col("Employee.Status") == 'Active') &
        (F.col("OrganizationalUnit.Division") == 'Global Wealth Management') &
        (F.col("OrganizationalUnit.Country") == 'Americas') &
        (~F.col("WorkLocation.locationName").like('%Home Worker%')) &
        (~F.col("WorkLocation.locationName").like('%No Workplace%'))
    ).select(df_gpin.gpin).distinct()

# Cache the result as it will be used in multiple joins
df_applicable_employees.cache()
print(f"Loaded and filtered {df_applicable_employees.count()} applicable employees.")

# --- Create validThinClientValues DataFrame (Finds top 2 build versions) ---
df_valid_thin_clients = df_thin_clients.filter(F.col("BuildVersion").isNotNull()) \
    .groupBy("BuildVersion") \
    .agg(F.countDistinct("EMMA").alias("events")) \
    .orderBy(F.col("events").desc()) \
    .limit(2) \
    .select("BuildVersion")

# Collect the valid versions into a Python list for efficient filtering later
valid_thin_client_list = [row.BuildVersion for row in df_valid_thin_clients.collect()]
print(f"Found valid thin client builds: {valid_thin_client_list}")
print("-" * 50)

# ==============================================================================
# 3. PROCESS TELEMETRY DATA
# ==============================================================================
print("Processing telemetry data...")

# --- Calculate dynamic latency thresholds (75th percentile) ---
df_latency_thresholds = df_telemetry_raw.join(df_applicable_employees, "gpin", "inner") \
    .groupBy("workLocationId", "connection") \
    .agg(F.expr("percentile_approx(latencyAverage, 0.75)").alias("latencyThreshold"))

# --- Get the most recent telemetry record for each user/connection ---
# This is far more efficient than the original SQL's multi-UNION approach.
window_spec_recent = Window.partitionBy("gpin", "connection").orderBy(F.col("timeStamp").desc())

df_telemetry_recent = df_telemetry_raw \
    .filter(F.col("timeStamp") >= blast_30_days_start) \
    .join(df_applicable_employees, "gpin", "inner") \
    .withColumn("rn", F.row_number().over(window_spec_recent)) \
    .filter(F.col("rn") == 1) \
    .drop("rn")

# --- Combine telemetry with thresholds for final processing ---
df_processed = df_telemetry_recent.join(
    df_latency_thresholds,
    ["workLocationId", "connection"],
    "left"
)

# Cache this processed DataFrame as it's the base for all issue checks
df_processed.cache()
print(f"Telemetry data processed and enriched. Base records for analysis: {df_processed.count()}")
print("-" * 50)

# ==============================================================================
# 4. DEFINE AND IDENTIFY ISSUES
# ==============================================================================
print("Identifying all issue types...")

# --- Define a reusable function to create a standardized issue DataFrame ---
def create_issue_df(issue_name, metric_col, filter_condition):
    """Filters the processed data for a specific issue and formats the output."""
    return df_processed \
        .filter(filter_condition) \
        .withColumn("issueType", F.lit(issue_name)) \
        .withColumn("metric", F.col(metric_col).cast("string")) \
        .select("gpin", "timeStamp", "connection", "remoteHostName", "localHostName", "issueType", "metric")

# --- Issue 1: Citrix ICA Latency ---
latency_condition = F.col("latencyAverage") > F.col("latencyThreshold")
df_latency_issues = create_issue_df("Citrix ICA Latency", "latencyAverage", latency_condition)

# --- Issue 2: Teams Speaker Change ---
speaker_change_condition = (F.col("teamSpeaker").isNotNull()) & (F.col("teamSpeaker") != "")
df_speaker_issues = create_issue_df("Teams Speaker Setup", "teamSpeaker", speaker_change_condition)

# --- Issue 3: Visual Display Setup ---
display_condition = (~F.col("monitorLayoutCount").isin([1, 2, 3])) | (F.col("monitorLayoutWidth") * F.col("monitorLayoutHeight") > 7864320)
df_display_issues = create_issue_df(
    "Visual Display Setup",
    F.concat_ws(" x ", F.col("monitorLayoutWidth"), F.col("monitorLayoutHeight")),
    display_condition
)

# --- Issue 4: Thin Client Build Version ---
thin_client_condition = (~F.col("thisClientBuildVersion").isin(valid_thin_client_list))
df_thin_client_issues = create_issue_df("Thin Client Build Version", "thisClientBuildVersion", thin_client_condition)

# --- Issue 5: Citrix Client Version ---
# Maintain your list of approved versions here or load from a config file/table.
approved_citrix_versions = ['22.3.100.4', '22.3.200.7'] # Example list
citrix_version_condition = (~F.col("citrixClientVersion").isin(approved_citrix_versions))
df_citrix_version_issues = create_issue_df("Citrix Client Version", "citrixClientVersion", citrix_version_condition)

print("All issue types defined and filtered.")
print("-" * 50)

# ==============================================================================
# 5. COMBINE ALL ISSUES AND ADD TIME PERIOD
# ==============================================================================
print("Combining all issues into a single report...")

# Union all the individual issue DataFrames together.
# unionByName is safer than a simple union as it matches columns by name.
final_report_df = df_latency_issues \
    .unionByName(df_speaker_issues) \
    .unionByName(df_display_issues) \
    .unionByName(df_thin_client_issues) \
    .unionByName(df_citrix_version_issues)

# Add the 'timePeriod' column based on the timestamp of the event
final_report_df_with_period = final_report_df.withColumn("timePeriod",
    F.when(F.col("timeStamp") >= blast_24_hours_start, "Last 24 hours")
     .when(F.col("timeStamp") >= blast_3_days_start, "Last 3 days")
     .when(F.col("timeStamp") >= blast_7_days_start, "Last 7 days")
     .otherwise("Last 30 days") # All data is already filtered for the last 30 days
)

print(f"Final report generated with {final_report_df_with_period.count()} total issues. ✅")
print("-" * 50)

# ==============================================================================
# 6. DISPLAY AND SAVE THE FINAL REPORT
# ==============================================================================
print("Displaying final report and saving to Delta table...")

# --- Display Final Results ---
# Use .display() in Databricks for rich, interactive visualizations.
# For scripting, you might use .show()
print("Top 20 rows of the final report:")
final_report_df_with_period.show(20, truncate=False)

# --- Save to a Delta Table (Production Best Practice) ---
# This creates a managed table in your Databricks metastore, enabling ACID
# transactions, time travel, and high-performance queries.
output_table_name = "prod_telemetry_issues_report"
final_report_df_with_period.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table_name)

print(f"\nReport successfully saved to Delta table: '{output_table_name}'")
print("Process complete!")
