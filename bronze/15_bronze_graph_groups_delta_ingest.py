# Databricks notebook: 15_bronze_graph_groups_delta_ingest  (🆕 NEW)
# MAGIC %run ./00_config_and_common

import requests, msal, json, datetime as dt
from pyspark.sql import functions as F, types as T

# ---------- CONFIG ----------
TENANT_ID = "<your-tenant-id>"
CLIENT_ID = "<app-client-id>"
CLIENT_SECRET = dbutils.secrets.get("keyvault-name","graph-api-client-secret")

GRAPH_SCOPE = ["https://graph.microsoft.com/.default"]
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
ENTITY = "groups"

BRONZE = "bronze"
OPS = "ops"

# ---------- AUTH ----------
def get_graph_token():
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_silent(GRAPH_SCOPE, account=None)
    if not result:
        result = app.acquire_token_for_client(scopes=GRAPH_SCOPE)
    if "access_token" not in result:
        raise Exception(f"Graph token error: {result.get('error_description')}")
    return result["access_token"]

token = get_graph_token()
headers = {"Authorization": f"Bearer {token}"}

# ---------- TOKEN STORE ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {OPS}.graph_delta_token_state (
  entity STRING,
  delta_token STRING,
  last_sync_time TIMESTAMP
) USING delta
""")

token_df = spark.table(f"{OPS}.graph_delta_token_state").filter(F.col("entity")==ENTITY)
token_row = token_df.head(1)

if token_row:
    delta_url = token_row[0]['delta_token']
    is_initial = False
else:
    delta_url = f"{GRAPH_BASE}/groups/delta"
    is_initial = True

# ---------- FETCH DELTAS ----------
print(f"Fetching delta for {ENTITY}: is_initial={is_initial}")

def fetch_deltas(url):
    all_groups = []
    while url:
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            raise Exception(f"Graph API error {resp.status_code}: {resp.text}")
        data = resp.json()
        all_groups.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
        delta_link = data.get("@odata.deltaLink")
        if delta_link:
            # store for later
            spark.createDataFrame(
                [(ENTITY, delta_link, dt.datetime.utcnow())],
                ["entity","delta_token","last_sync_time"]
            ).write.mode("overwrite").option("replaceWhere", f"entity='{ENTITY}'")\
             .saveAsTable(f"{OPS}.graph_delta_token_state")
    return all_groups

groups = fetch_deltas(delta_url)
print(f"Fetched {len(groups)} groups")

# ---------- PARSE STRUCTURE ----------
def extract_members_owners(groups):
    members, owners, group_flat = [], [], []
    for g in groups:
        gid = g.get("id")
        if not gid:
            continue
        # handle deletions
        if "@removed" in g:
            group_flat.append({"group_id": gid, "removed": True})
            continue
        group_flat.append({
            "group_id": gid,
            "display_name": g.get("displayName"),
            "mail": g.get("mail"),
            "security_enabled": g.get("securityEnabled"),
            "mail_enabled": g.get("mailEnabled"),
            "group_types": g.get("groupTypes"),
            "description": g.get("description"),
            "raw_json": json.dumps(g)
        })
        # membership changes
        for m in g.get("members@delta", []):
            if "@removed" in m:
                members.append({"group_id": gid, "member_id": m.get("id"), "action":"remove"})
            else:
                members.append({"group_id": gid, "member_id": m.get("id"), "action":"add"})
        # ownership changes
        for o in g.get("owners@delta", []):
            if "@removed" in o:
                owners.append({"group_id": gid, "owner_id": o.get("id"), "action":"remove"})
            else:
                owners.append({"group_id": gid, "owner_id": o.get("id"), "action":"add"})
    return group_flat, members, owners

group_flat, members, owners = extract_members_owners(groups)

# ---------- WRITE TO BRONZE ----------
for tbl, data, schema in [
    (f"{BRONZE}.graph_groups_delta", group_flat, None),
    (f"{BRONZE}.graph_groups_members_delta", members, None),
    (f"{BRONZE}.graph_groups_owners_delta", owners, None)
]:
    if len(data)==0: 
        print(f"No rows for {tbl}")
        continue
    df = spark.createDataFrame(data)
    df = df.withColumn("ingest_ts", F.current_timestamp())
    df.write.mode("append").saveAsTable(tbl)
    print(f"Written {df.count()} rows to {tbl}")
