# ==============================================================================
#  Databricks Telemetry Issue Reporting Script (Corrected & Complete)
# ==============================================================================
# This script is a corrected and complete translation of the provided SQL query,
# optimized for a production Databricks environment. It includes all crucial
# logic, such as IQR for latency, specific issue criteria, and dynamic version
# validation.
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
# CRITICAL: For production, store credentials in Databricks secrets.
# Example: jdbc_user = dbutils.secrets.get(scope="your_secret_scope", key="your_jdbc_user_key")

jdbc_hostname = "<your_sql_server_hostname>"
jdbc_port = 1433
jdbc_database = "<your_database_name>"
jdbc_user = "<your_username>"
jdbc_password = "<your_password>" # Replace with dbutils.secrets.get() in production

# Create the JDBC URL for connecting to SQL Server
jdbc_url = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};databaseName={jdbc_database}"

# Define connection properties
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

# Load all necessary tables from SQL Server
df_gpin = read_sql_table("gpin")
df_employee = read_sql_table("Employee")
df_org_unit = read_sql_table("OrganizationalUnit")
df_work_location = read_sql_table("WorkLocation")
df_telemetry_raw = read_sql_table("MyHubRemoteAnaylics")
df_kupa_builds = read_sql_table("KupaOnPremThinClientBuildVersion")
df_supported_citrix_versions = read_sql_table("MyHubRemoteAnaylicsSupportedValues")
df_thin_clients = read_sql_table("ThinClients")

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

df_applicable_employees.cache()
print(f"Loaded and filtered {df_applicable_employees.count()} applicable employees.")

# --- Create validThinClientValues DataFrame (Finds top 2 build versions) ---
df_valid_thin_clients = df_thin_clients.filter(F.col("BuildVersion").isNotNull()) \
    .groupBy("BuildVersion") \
    .agg(F.countDistinct("EMMA").alias("events")) \
    .orderBy(F.col("events").desc()) \
    .limit(2) \
    .select("BuildVersion")
valid_thin_client_list = [row.BuildVersion for row in df_valid_thin_clients.collect()]
print(f"Found valid thin client builds: {valid_thin_client_list}")

# --- Get Supported Citrix Versions ---
# The SQL uses `CROSS APPLY string_split`. We can replicate this by exploding a column if needed.
# Assuming `SupportedValues` table has one version per row in the `Value` column.
supported_citrix_list = [row.Value for row in df_supported_citrix_versions.select("Value").distinct().collect()]
print(f"Found {len(supported_citrix_list)} supported Citrix versions.")
print("-" * 50)

# ==============================================================================
# 3. PREPARE & ENRICH TELEMETRY DATA
# ==============================================================================
print("Preparing and enriching telemetry data...")

# --- Filter telemetry to the last 30 days and for applicable employees ---
df_telemetry_filtered = df_telemetry_raw \
    .join(df_applicable_employees, "gpin", "inner") \
    .filter(F.col("timeStamp") >= blast_30_days_start)

# --- Replicate complex logic to derive 'thisClientBuildVersion' ---
# This translates the complex CASE statement with CHARINDEX and SUBSTRING from the SQL.
df_telemetry_with_build = df_telemetry_filtered.withColumn("thisClientBuildVersion",
    F.when(F.locate("','", F.col("audioDeviceComposite")) > 0,
        F.when(F.substring_index(F.col("audioDeviceComposite"), "','", 1) == 'T1', F.lit(None))
         .otherwise(F.replace(F.substring_index(F.col("audioDeviceComposite"), "','", 1), "T2', '", ""))
    ).otherwise(
        F.when(F.locate("','", F.col("audioDeviceComposite")) == 0,
            F.when(F.substring_index(F.col("audioDeviceComposite"), ",", 1) == 'T1', F.lit(None))
             .otherwise(F.replace(F.left(F.col("audioDeviceComposite"), F.locate(",", F.col("audioDeviceComposite")) - 1), "T1', '", ""))
        )
    )
).join(df_kupa_builds, (F.col("r.remoteHostName") == F.col("t.HostName")) & (F.col("r.gateway") == F.col("t.Gateway")), "left_outer") \
 .withColumn("finalClientBuildVersion", F.coalesce(F.col("thisClientBuildVersion"), F.col("buildVersion")))


# --- Calculate Latency Thresholds using IQR Method ---
# This is a critical piece of logic missed previously.
df_percentiles = df_telemetry_filtered.groupBy("workLocationId", "connection").agg(
    F.expr("percentile_approx(latencyAverage, 0.25)").alias("q1"),
    F.expr("percentile_approx(latencyAverage, 0.75)").alias("q3")
)

df_latency_thresholds = df_percentiles.withColumn("iqr", F.col("q3") - F.col("q1")) \
    .withColumn("latencyUpperBound", F.col("q3") + (F.lit(1.5) * F.col("iqr"))) \
    .select("workLocationId", "connection", "latencyUpperBound")

# --- Join enriched telemetry with latency thresholds ---
df_processed = df_telemetry_with_build.join(
    df_latency_thresholds,
    ["workLocationId", "connection"],
    "left"
)

df_processed.cache()
print(f"Telemetry data processed and enriched. Base records for analysis: {df_processed.count()}")
print("-" * 50)

# ==============================================================================
# 4. DEFINE AND IDENTIFY ISSUES
# ==============================================================================
print("Identifying all issue types from processed data...")

# --- Reusable function to create a standardized issue DataFrame ---
def create_issue_df(df_input, issue_name, metric_col, filter_condition):
    return df_input.filter(filter_condition) \
        .withColumn("issueType", F.lit(issue_name)) \
        .withColumn("metric", metric_col.cast("string")) \
        .select("gpin", "timeStamp", "connection", "remoteHostName", "localHostName", "issueType", "metric")

# --- Issue 1: Citrix ICA Latency (using correct IQR upperBound) ---
latency_condition = F.col("latencyAverage") > F.col("latencyUpperBound")
df_latency_issues = create_issue_df(df_processed, "Citrix ICA Latency", F.col("latencyAverage"), latency_condition)

# --- Issue 2: Teams Speaker Change (with specific exclusions) ---
speaker_exclusions = [
    "VDI Optimized", "Jabra", "Plantronics", "Logitech", "Sennheiser",
    "Poly", "Nureva", "Huddly", "Bose", "Crestron", "Yealink", "EPOS",
    "HP", "Lenovo", "Dell", "Microsoft"
]
speaker_change_condition = F.col("teamSpeaker") != ""
for exclusion in speaker_exclusions:
    speaker_change_condition &= ~F.col("teamSpeaker").like(f"%{exclusion}%")
df_speaker_issues = create_issue_df(df_processed, "Teams Speaker Setup", F.col("teamSpeaker"), speaker_change_condition)

# --- Issue 3: Visual Display Setup (with correct pixel and dimension logic) ---
display_condition = (
    (F.col("monitorLayoutWidth") * F.col("monitorLayoutHeight") > 921600) |
    (F.col("monitorLayoutWidth") > 3840) |
    (F.col("monitorLayoutHeight") > 2160)
)
df_display_issues = create_issue_df(
    df_processed,
    "Visual Display Setup",
    F.concat_ws(" x ", F.col("monitorLayoutCount"), F.col("monitorLayoutWidth"), F.col("monitorLayoutHeight")),
    display_condition
)

# --- Issue 4: Thin Client Build Version ---
thin_client_condition = (~F.col("finalClientBuildVersion").isin(valid_thin_client_list))
df_thin_client_issues = create_issue_df(df_processed, "Thin Client Build Version", F.col("finalClientBuildVersion"), thin_client_condition)

# --- Issue 5: Citrix Client Version (checking against supported list) ---
citrix_version_condition = (~F.trim(F.col("citrixClientVersion")).isin(supported_citrix_list))
df_citrix_version_issues = create_issue_df(df_processed, "Citrix Client Version", F.col("citrixClientVersion"), citrix_version_condition)

print("All issue types defined and filtered.")
print("-" * 50)

# ==============================================================================
# 5. COMBINE ALL ISSUES AND ADD TIME PERIOD
# ==============================================================================
print("Combining all issues into a single report...")

# Union all the individual issue DataFrames together.
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
     .otherwise("Last 30 days")
)

print(f"Final report generated with {final_report_df_with_period.count()} total issues. ✅")
print("-" * 50)

# ==============================================================================
# 6. DISPLAY AND SAVE THE FINAL REPORT
# ==============================================================================
print("Displaying final report and saving to Delta table...")

# --- Display Final Results ---
print("Top 20 rows of the final report:")
final_report_df_with_period.show(20, truncate=False)

# --- Save to a Delta Table (Production Best Practice) ---
output_table_name = "prod_telemetry_issues_report"
final_report_df_with_period.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table_name)

print(f"\nReport successfully saved to Delta table: '{output_table_name}'")
print("Process complete!")
