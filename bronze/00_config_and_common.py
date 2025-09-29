# Databricks notebook: 00_config_and_common

# --- config ---
CATALOG = "ubs_sec"
BRONZE = f"{CATALOG}.bronze"
OPS    = f"{CATALOG}.ops"

# ADLS raw folders populated by ADF (MGDC)
RAW_BASE = "abfss://raw@<yourstorageaccount>.dfs.core.windows.net"
MGDC_PATHS = {
    "sharepoint_sites_v1":      f"{RAW_BASE}/mgdc/sharepoint/sites_v1/",
    "sharepoint_permissions_v1":f"{RAW_BASE}/mgdc/sharepoint/permissions_v1/",
    "sharepoint_groups_v1":     f"{RAW_BASE}/mgdc/sharepoint/groups_v1/",
    "group_details_v0":         f"{RAW_BASE}/mgdc/aad/group_details_v0/",
    "group_members_v0":         f"{RAW_BASE}/mgdc/aad/group_members_v0/",
    "group_owners_v0":          f"{RAW_BASE}/mgdc/aad/group_owners_v0/",
}

# landing area for Graph delta JSON files (optional; we also write to Delta tables)
GRAPH_LANDING = f"{RAW_BASE}/graph/"

# --- tables ---
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {OPS}.graph_delta_tokens (
  scope STRING PRIMARY KEY,
  delta_link STRING,
  last_success_ts TIMESTAMP,
  snapshot_filter_from TIMESTAMP
) TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {OPS}.ingestion_runs (
  run_id STRING,
  source STRING,
  entity STRING,
  status STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  rows_ingested BIGINT,
  notes STRING
)
""")

# --- helpers ---
import json, time, uuid, datetime as dt
from typing import Dict, Any, Iterable, Optional
from pyspark.sql import functions as F

def new_run_id(prefix="ingest"):
    return f"{prefix}_{dt.datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"

def log_run(run_id, source, entity, status, rows=0, notes=None, started_at=None):
    now = dt.datetime.utcnow()
    started_at = started_at or now
    spark.createDataFrame([{
        "run_id": run_id, "source": source, "entity": entity,
        "status": status, "started_at": started_at, "ended_at": now,
        "rows_ingested": rows, "notes": notes
    }]).write.mode("append").saveAsTable(f"{OPS}.ingestion_runs")

# --- MSAL (Microsoft library) ---
import msal, requests

TENANT_ID = dbutils.secrets.get("mgmt-kv", "aad-tenant-id")
CLIENT_ID = dbutils.secrets.get("mgmt-kv", "graph-client-id")
CLIENT_SECRET = dbutils.secrets.get("mgmt-kv", "graph-client-secret")
GRAPH_SCOPE = ["https://graph.microsoft.com/.default"]

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
APP = msal.ConfidentialClientApplication(
    CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET
)

def graph_token():
    result = APP.acquire_token_silent(GRAPH_SCOPE, account=None)
    if not result:
        result = APP.acquire_token_for_client(scopes=GRAPH_SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"MSAL auth failed: {result}")
    return result["access_token"]

def graph_get(url, params=None, headers=None, max_retries=5):
    tok = graph_token()
    h = {"Authorization": f"Bearer {tok}"}
    if headers: h.update(headers)
    backoff = 1.0
    for _ in range(max_retries):
        r = requests.get(url, params=params, headers=h, timeout=60)
        if r.status_code in (200, 201):
            return r.json()
        if r.status_code in (429, 503, 504):
            time.sleep(backoff); backoff = min(backoff*2, 60)
            continue
        raise RuntimeError(f"Graph GET {url} failed: {r.status_code} {r.text}")
    raise RuntimeError(f"Graph GET {url} exhausted retries")
