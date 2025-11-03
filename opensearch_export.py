from dotenv import load_dotenv
import os
import awswrangler as wr
from opensearchpy import OpenSearch
import pandas as pd
import time
import json
import csv
import os

# --- Load environment variables from .env file if present ---
load_dotenv()

# --- Configuration ---
HOST = os.getenv("OPENSEARCH_SOURCE_HOST")
PORT = 443
USERNAME = os.getenv("OPENSEARCH_SOURCE_USER")
PASSWORD = os.getenv("OPENSEARCH_SOURCE_PASS")
SCROLL_TIMEOUT = "2m"  # how long each scroll context stays alive
BATCH_SIZE = 1000      # number of docs per scroll batch

# --- Connect to OpenSearch ---
search_client = OpenSearch(
    hosts=[{"host": HOST, "port": PORT}],
    http_auth=(USERNAME, PASSWORD),
    use_ssl=True,
    verify_certs=True,
)

# --- List of indexes to export ---
indexes = [
  "new-forms-ux-ilt-test_workflow_process",
  "teks.dev_workflow_process_task",
  "new-forms-ux-ilt-demo_workflow_process_task",
  "kpi-execution-date_workflow_process",
  "cd-1920_asset_status",
  "new-forms-ux-ilt-demo_workflow_process",
  "cd-1920_workflow_process",
  "cd-1920_workflow_process_task",
  "teks.dev_workflow_process",
  ".opendistro_security",
  "new-forms-ux-ilt-test_asset_status",
  "billing_workflow_process_task",
  "wizard-past-date_asset_status",
  "workflow_process_task",
  "cd-1849_workflow_process_task",
  "labels_workflow_process",
  "new-forms-ux-ilt_workflow_process",
  "wizard-past-date_workflow_process",
  "kpi-execution-date_workflow_process_task",
  ".kibana_-1657425983_capptions_1",
  "wizard-past-date_workflow_process_task",
  "new-forms-ux-ilt_workflow_process_task",
  "teks.dev_asset_status",
  "igor-release_asset_status",
  "igor-release_workflow_process_task",
  "new-forms-ux-ilt-demo_asset_status",
  "new-assets_workflow_process_task",
  "new-forms-ux-ilt-test_workflow_process_task",
  "kpi-execution-date_asset_status",
  "labels_workflow_process_task",
  "tenant-wallet",
  ".kibana_1",
  "workflow_process",
  "apple-auto-onboard_workflow_process_task",
  "billing_asset_status",
  "new-forms-ux-ilt_asset_status",
  "igor-release_workflow_process",
  "cd-1849_workflow_process",
  "labels_asset_status",
  "new-assets_asset_status",
  "cd-1849_asset_status",
  "new-assets_workflow_process",
]

# --- Export loop ---
for index in indexes:
    time.sleep(2)
    print(f"\n🚀 Exporting index: {index}")

    output_path = f"{index}.csv"
    total_docs = 0
    first_write = True

    try:
        # 1️⃣ Initial search with scroll
        response = search_client.search(
            index=index,
            scroll=SCROLL_TIMEOUT,
            size=BATCH_SIZE,
            body={"query": {"match_all": {}}},
        )

        scroll_id = response.get("_scroll_id")
        hits = response["hits"]["hits"]

        if not hits:
            print(f"⚠️ Index '{index}' returned no results, skipping.")
            continue

        # 2️⃣ Process first batch
        while hits:
            # Extract _source fields into DataFrame
            batch_data = [h["_source"] for h in hits]
            df = pd.DataFrame(batch_data)

            # Write batch to CSV
            df.to_csv(
                output_path,
                index=False,
                mode="w" if first_write else "a",
                header=first_write,
                quoting=csv.QUOTE_ALL,
            )

            first_write = False
            total_docs += len(df)
            print(f"   → {total_docs} documents exported so far...", end="\r")

            # Get next batch
            response = search_client.scroll(
                scroll_id=scroll_id,
                scroll=SCROLL_TIMEOUT,
            )
            scroll_id = response.get("_scroll_id")
            hits = response["hits"]["hits"]

        # 3️⃣ Clear scroll context
        if scroll_id:
            search_client.clear_scroll(scroll_id=scroll_id)

        print(f"\n✅ Exported {total_docs} documents from '{index}' to {output_path}")

    except Exception as e:
        print(f"❌ Failed to export '{index}': {e}")
