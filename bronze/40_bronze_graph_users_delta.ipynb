# Databricks notebook: 40_bronze_graph_users_delta
# MAGIC %run ./00_config_and_common

scope = "users"
row = spark.table(f"{OPS}.graph_delta_tokens").where(F.col("scope")==scope).collect()

def seed_users():
    url = "https://graph.microsoft.com/v1.0/users/delta"
    batch_id = new_run_id("graph_users_seed")
    start = dt.datetime.utcnow()
    total = 0
    next_link = None
    try:
        while True:
            payload = graph_get(next_link or url)
            vals = payload.get("value", [])
            total += len(vals)
            if vals:
                recs = [{
                    "id": u.get("id"),
                    "raw_json": json.dumps(u),
                    "user_principal_name": u.get("userPrincipalName"),
                    "mail": u.get("mail"),
                    "account_enabled": u.get("accountEnabled"),
                    "last_modified": u.get("lastModifiedDateTime"),
                    "delta_batch_id": batch_id,
                    "delta_source": "users",
                    "received_ts": dt.datetime.utcnow()
                } for u in vals]
                spark.createDataFrame(recs).withColumn("last_modified", F.to_timestamp("last_modified")) \
                    .write.mode("append").saveAsTable(f"{BRONZE}.graph_users_delta")
            next_link = payload.get("@odata.nextLink")
            delta_link = payload.get("@odata.deltaLink")
            if next_link:
                continue
            spark.sql(f"DELETE FROM {OPS}.graph_delta_tokens WHERE scope = '{scope}'")
            spark.createDataFrame([{
                "scope": scope, "delta_link": delta_link, "last_success_ts": dt.datetime.utcnow(), "snapshot_filter_from": None
            }]).write.mode("append").saveAsTable(f"{OPS}.graph_delta_tokens")
            log_run(batch_id, "GRAPH", "users-seed", "SUCCESS", rows=total, started_at=start)
            break
    except Exception as e:
        log_run(batch_id, "GRAPH", "users-seed", "FAILED", notes=str(e), started_at=start)
        raise

def incr_users(delta_link: str):
    batch_id = new_run_id("graph_users_incr")
    start = dt.datetime.utcnow()
    total = 0
    nl = delta_link
    try:
        while True:
            payload = graph_get(nl)
            vals = payload.get("value", [])
            total += len(vals)
            if vals:
                recs = [{
                    "id": u.get("id"),
                    "raw_json": json.dumps(u),
                    "user_principal_name": u.get("userPrincipalName"),
                    "mail": u.get("mail"),
                    "account_enabled": u.get("accountEnabled"),
                    "last_modified": u.get("lastModifiedDateTime"),
                    "delta_batch_id": batch_id,
                    "delta_source": "users",
                    "received_ts": dt.datetime.utcnow()
                } for u in vals]
                spark.createDataFrame(recs).withColumn("last_modified", F.to_timestamp("last_modified")) \
                    .write.mode("append").saveAsTable(f"{BRONZE}.graph_users_delta")
            nl = payload.get("@odata.nextLink")
            new_delta = payload.get("@odata.deltaLink")
            if nl:
                continue
            spark.sql(f"UPDATE {OPS}.graph_delta_tokens SET delta_link = '{new_delta}', last_success_ts = current_timestamp() WHERE scope = '{scope}'")
            log_run(batch_id, "GRAPH", "users-incremental", "SUCCESS", rows=total, started_at=start)
            break
    except Exception as e:
        log_run(batch_id, "GRAPH", "users-incremental", "FAILED", notes=str(e), started_at=start)
        raise

# run logic
if not row:
    seed_users()
else:
    incr_users(row[0]["delta_link"])
