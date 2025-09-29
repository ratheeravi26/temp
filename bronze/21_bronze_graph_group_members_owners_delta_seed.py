# Databricks notebook: 21_bronze_graph_group_members_owners_delta_seed
# MAGIC %run ./00_config_and_common

from pyspark.sql import functions as F

# 1) get changed group ids from last seed batch
latest_batch = spark.table(f"{BRONZE}.graph_groups_delta") \
    .agg(F.max("delta_batch_id").alias("b")).first()["b"]

if latest_batch is None:
    dbutils.notebook.exit("No changed groups to seed.")

group_ids = [r["id"] for r in spark.table(f"{BRONZE}.graph_groups_delta").where(F.col("delta_batch_id")==latest_batch).select("id").distinct().collect()]
print(f"Changed groups: {len(group_ids)}")

def seed_collection(group_id: str, col: str):  # col in {'members','owners'}
    base = f"https://graph.microsoft.com/v1.0/groups/{group_id}/{col}/delta"
    batch_id = new_run_id(f"graph_{col}_seed")
    rows = 0
    next_link = None
    start = dt.datetime.utcnow()
    try:
        while True:
            payload = graph_get(next_link or base)
            vals = payload.get("value", [])
            rows += len(vals)
            records = []
            for v in vals:
                if col == "members":
                    pid = v.get("id")
                    ptype = v.get("@odata.type", "")
                    if "#microsoft.graph.user" in ptype:
                        ptype = "User"
                    elif "#microsoft.graph.group" in ptype:
                        ptype = "Group"
                    else:
                        ptype = "Other"
                    records.append({
                        "group_id": group_id,
                        "principal_id": pid,
                        "principal_type": ptype,
                        "raw_json": json.dumps(v),
                        "delta_batch_id": batch_id,
                        "delta_source": "members",
                        "snapshot_filter_from": None,
                        "received_ts": dt.datetime.utcnow()
                    })
                else:
                    records.append({
                        "group_id": group_id,
                        "owner_id": v.get("id"),
                        "raw_json": json.dumps(v),
                        "delta_batch_id": batch_id,
                        "delta_source": "owners",
                        "snapshot_filter_from": None,
                        "received_ts": dt.datetime.utcnow()
                    })
            if records:
                if col == "members":
                    spark.createDataFrame(records).write.mode("append").saveAsTable(f"{BRONZE}.graph_group_members_delta")
                else:
                    spark.createDataFrame(records).write.mode("append").saveAsTable(f"{BRONZE}.graph_group_owners_delta")
            next_link = payload.get("@odata.nextLink")
            delta_link = payload.get("@odata.deltaLink")
            if next_link:
                continue
            # save per-group token
            scope = f"group:{group_id}:{col}"
            spark.sql(f"DELETE FROM {OPS}.graph_delta_tokens WHERE scope = '{scope}'")
            spark.createDataFrame([{
                "scope": scope,
                "delta_link": delta_link,
                "last_success_ts": dt.datetime.utcnow(),
                "snapshot_filter_from": None
            }]).write.mode("append").saveAsTable(f"{OPS}.graph_delta_tokens")
            log_run(batch_id, "GRAPH", f"{col}-delta-seed", "SUCCESS", rows=rows, notes=f"group={group_id}", started_at=start)
            break
    except Exception as e:
        log_run(batch_id, "GRAPH", f"{col}-delta-seed", "FAILED", notes=f"group={group_id} err={e}", started_at=start)
        raise

# 2) seed for each changed group (you can parallelize with a job cluster)
for gid in group_ids:
    seed_collection(gid, "members")
    seed_collection(gid, "owners")
