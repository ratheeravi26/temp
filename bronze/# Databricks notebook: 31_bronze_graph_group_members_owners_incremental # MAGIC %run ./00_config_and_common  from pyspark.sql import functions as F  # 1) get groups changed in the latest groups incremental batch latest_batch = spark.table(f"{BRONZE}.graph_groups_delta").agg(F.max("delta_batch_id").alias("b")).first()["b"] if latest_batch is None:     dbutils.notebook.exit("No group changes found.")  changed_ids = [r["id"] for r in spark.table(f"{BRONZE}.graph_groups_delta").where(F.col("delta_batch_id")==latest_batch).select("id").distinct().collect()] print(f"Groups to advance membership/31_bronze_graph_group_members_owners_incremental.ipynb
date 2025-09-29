# Databricks notebook: 31_bronze_graph_group_members_owners_incremental
# MAGIC %run ./00_config_and_common

from pyspark.sql import functions as F

# 1) get groups changed in the latest groups incremental batch
latest_batch = spark.table(f"{BRONZE}.graph_groups_delta").agg(F.max("delta_batch_id").alias("b")).first()["b"]
if latest_batch is None:
    dbutils.notebook.exit("No group changes found.")

changed_ids = [r["id"] for r in spark.table(f"{BRONZE}.graph_groups_delta").where(F.col("delta_batch_id")==latest_batch).select("id").distinct().collect()]
print(f"Groups to advance membership/owners: {len(changed_ids)}")

def advance(scope_prefix: str, table: str, is_members: bool):
    for gid in changed_ids:
        scope = f"group:{gid}:{'members' if is_members else 'owners'}"
        rows = spark.table(f"{OPS}.graph_delta_tokens").where(F.col("scope")==scope).collect()
        if not rows:
            # if token missing (newly interesting group), seed it (call seed notebook or do first delta here)
            print(f"No token for {scope}, seeding...")
            dbutils.notebook.run("21_bronze_graph_group_members_owners_delta_seed", 0)
            continue
        delta = rows[0]["delta_link"]
        batch_id = new_run_id(f"graph_{'members' if is_members else 'owners'}_incr")
        start = dt.datetime.utcnow()
        total = 0
        next_link = delta
        try:
            while True:
                payload = graph_get(next_link)
                vals = payload.get("value", [])
                total += len(vals)
                if vals:
                    if is_members:
                        recs = []
                        for v in vals:
                            pid = v.get("id")
                            ptype = v.get("@odata.type", "")
                            if "#microsoft.graph.user" in ptype:
                                ptype = "User"
                            elif "#microsoft.graph.group" in ptype:
                                ptype = "Group"
                            else:
                                ptype = "Other"
                            recs.append({
                                "group_id": gid, "principal_id": pid, "principal_type": ptype,
                                "raw_json": json.dumps(v), "delta_batch_id": batch_id,
                                "delta_source": "members", "snapshot_filter_from": None, "received_ts": dt.datetime.utcnow()
                            })
                        spark.createDataFrame(recs).write.mode("append").saveAsTable(f"{BRONZE}.graph_group_members_delta")
                    else:
                        recs = [{
                            "group_id": gid, "owner_id": v.get("id"), "raw_json": json.dumps(v),
                            "delta_batch_id": batch_id, "delta_source": "owners",
                            "snapshot_filter_from": None, "received_ts": dt.datetime.utcnow()
                        } for v in vals]
                        spark.createDataFrame(recs).write.mode("append").saveAsTable(f"{BRONZE}.graph_group_owners_delta")
                next_link = payload.get("@odata.nextLink")
                new_delta = payload.get("@odata.deltaLink")
                if next_link:
                    continue
                # update token
                spark.sql(f"""
                    UPDATE {OPS}.graph_delta_tokens
                    SET delta_link = '{new_delta}', last_success_ts = current_timestamp()
                    WHERE scope = '{scope}'
                """)
                log_run(batch_id, "GRAPH", f"{'members' if is_members else 'owners'}-incremental", "SUCCESS", rows=total, notes=f"group={gid}", started_at=start)
                break
        except Exception as e:
            log_run(batch_id, "GRAPH", f"{'members' if is_members else 'owners'}-incremental", "FAILED", notes=f"group={gid} err={e}", started_at=start)
            raise

# 2) advance
advance("group", f"{BRONZE}.graph_group_members_delta", True)
advance("group", f"{BRONZE}.graph_group_owners_delta", False)
