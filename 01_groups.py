# 01_groups.py
from shared_utils import graph_get, get_graph_token, audit_scope, write_bronze, silver_from_bronze, upsert_token
import argparse

# --- Parse notebook parameters ---
dbutils.widgets.text("mode", "full")  # "full" or "delta"
mode = dbutils.widgets.get("mode").lower()

GRAPH_RESOURCE = "https://graph.microsoft.com"
GRAPH_VERSION = "v1.0"
PAGE_SIZE = 999
scope = "groups"

# --- Determine URL based on mode ---
if mode == "full":
    url = f"{GRAPH_RESOURCE}/{GRAPH_VERSION}/groups?$top={PAGE_SIZE}"
elif mode == "delta":
    # Fetch last stored deltaLink
    delta_link_record = upsert_token(scope, read=True)
    if not delta_link_record:
        raise ValueError("No deltaLink found. Run initial full load first.")
    url = delta_link_record
    url = url if "?" in url else url + f"?$top={PAGE_SIZE}"
else:
    raise ValueError(f"Unknown mode: {mode}")

# --- Audit start ---
audit_scope(scope, "START", f"Starting {mode} load from {url}")

# --- Fetch loop ---
page_count = 0
total = 0

try:
    while url:
        r = graph_get(url)
        data = r.json()
        items = data.get("value", [])
        page_count += 1
        total += len(items)
        
        # Log page size for ops visibility
        audit_scope(scope, "PAGE", f"Page {page_count} returned {len(items)} items")
        
        # Save to bronze
        write_bronze(items)
        
        # Next link or deltaLink
        next_link = data.get("@odata.nextLink")
        delta_link = data.get("@odata.deltaLink")
        if next_link:
            url = next_link + f"&$top={PAGE_SIZE}"
        else:
            url = None
            # Promote bronze → silver
            silver_from_bronze()
            # Store deltaLink for future incremental loads
            if delta_link:
                upsert_token(scope, token=delta_link, provisional="", status="committed")
    
    audit_scope(scope, "SUCCESS", f"Fetched {total} groups across {page_count} pages")
except Exception as e:
    audit_scope(scope, "FAIL", f"Error: {str(e)}")
    raise
