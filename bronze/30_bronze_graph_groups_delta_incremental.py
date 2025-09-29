# Databricks notebook: 30_bronze_graph_groups_delta_incremental
# MAGIC %run ./00_config_and_common

row = spark.table(f"{OPS}.graph_delta_tokens").where("scope = 'groups-container'").limit(1).collect()
assert row, "No delta token for groups-container. Run 20_bronze_graph_groups_delta_seed_from_snapshot first."
delta_link = row[0]["delta_link"]

batch_id = new_run_id("graph_groups_incr")
start = dt.datetime.utcnow()
changed_groups = []
next_link = delta_link
try:
    while True:
        payload = graph_get(next_link)
        vals = payload.get("value", [])
        for g in vals:
            changed_groups.append({
                "id": g.get("id"),
                "raw_json": json.dumps(g),
                "last_modified": g.get("lastModifiedDateTime"),
                "delta_batch_id": batch_id,
                "delta_source": "groups-container",
                "snapshot_filter_from": None,
                "received_ts": dt.datetime.utcnow()
            })
        next_link = payload.get("@odata.nextLink")
        new_delta = payload.get("@odata.deltaLink")
        if next_link:
            continue
        # write results + update token
        if changed_groups:
            df = spark.createDataFrame(changed_groups).withColumn("last_modified", F.to_timestamp("last_modified"))
            df.write.mode("append").saveAsTable(f"{BRONZE}.graph_groups_delta")
        spark.sql(f"UPDATE {OPS}.graph_delta_tokens SET delta_link = '{new_delta}', last_success_ts = current_timestamp() WHERE scope = 'groups-container'")
        log_run(batch_id, "GRAPH", "groups-incremental", "SUCCESS", rows=len(changed_groups), started_at=start)
        break
except Exception as e:
    log_run(batch_id, "GRAPH", "groups-incremental", "FAILED", notes=str(e), started_at=start)
    raise
