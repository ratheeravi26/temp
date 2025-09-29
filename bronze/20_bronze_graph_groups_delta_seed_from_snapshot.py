# Databricks notebook: 20_bronze_graph_groups_delta_seed_from_snapshot
# MAGIC %run ./00_config_and_common

from pyspark.sql import functions as F

# 1) determine MGDC snapshot time (you can set this explicitly or compute last successful MGDC run)
# For simplicity, read the max ingest_ts from MGDC groups as the snapshot
snap_ts = spark.table(f"{BRONZE}.mgdc_group_details_v0").agg(F.max("ingest_ts")).first()[0]
assert snap_ts is not None, "No MGDC baseline found in bronze.mgdc_group_details_v0"

snapshot_iso = snap_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
print("Using MGDC snapshot time:", snapshot_iso)

# 2) call groups delta with filter on lastModifiedDateTime >= snapshot
url = "https://graph.microsoft.com/v1.0/groups/delta"
params = {"$filter": f"lastModifiedDateTime ge {snapshot_iso}"}

changed_groups = []
next_link = None
batch_id = new_run_id("graph_groups_seed")
start = dt.datetime.utcnow()

try:
    while True:
        payload = graph_get(next_link or url, params=None if next_link else params)
        value = payload.get("value", [])
        for g in value:
            changed_groups.append({
                "id": g.get("id"),
                "raw_json": json.dumps(g),
                "last_modified": g.get("lastModifiedDateTime"),
                "delta_batch_id": batch_id,
                "delta_source": "groups-container",
                "snapshot_filter_from": snap_ts,
                "received_ts": dt.datetime.utcnow()
            })
        next_link = payload.get("@odata.nextLink")
        delta_link = payload.get("@odata.deltaLink")
        if next_link:
            continue
        # last page: persist deltaLink
        spark.sql(f"DELETE FROM {OPS}.graph_delta_tokens WHERE scope = 'groups-container'")
        spark.createDataFrame([{
            "scope": "groups-container",
            "delta_link": delta_link,
            "last_success_ts": dt.datetime.utcnow(),
            "snapshot_filter_from": snap_ts
        }]).write.mode("append").saveAsTable(f"{OPS}.graph_delta_tokens")
        break

    # 3) write changed groups to bronze
    if changed_groups:
        df = spark.createDataFrame(changed_groups) \
                 .withColumn("last_modified", F.to_timestamp("last_modified"))
        df.write.mode("append").saveAsTable(f"{BRONZE}.graph_groups_delta")
    else:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {BRONZE}.graph_groups_delta (id STRING, raw_json STRING, last_modified TIMESTAMP, delta_batch_id STRING, delta_source STRING, snapshot_filter_from TIMESTAMP, received_ts TIMESTAMP)")
    log_run(batch_id, "GRAPH", "groups-delta-seed", "SUCCESS", rows=len(changed_groups), started_at=start)
except Exception as e:
    log_run(batch_id, "GRAPH", "groups-delta-seed", "FAILED", notes=str(e), started_at=start)
    raise
