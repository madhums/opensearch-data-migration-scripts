from dotenv import load_dotenv
import os
import ast
import json
from opensearchpy import OpenSearch, RequestsHttpConnection
import awswrangler as wr
import pandas as pd
import csv

# --- Load environment variables from .env file if present ---
load_dotenv()

HOST = os.getenv("OPENSEARCH_TARGET_HOST")
USERNAME = os.getenv("OPENSEARCH_TARGET_USER")
PASSWORD = os.getenv("OPENSEARCH_TARGET_PASS")

# Create client manually
client = OpenSearch(
    hosts=[{"host": HOST, "port": 9200}],
    http_auth=(USERNAME, PASSWORD),
    use_ssl=True,          # set to True if your server uses HTTPS
    verify_certs=False,     # only needed if using self-signed SSL
    ssl_assert_hostname=False,
    ssl_show_warn=False,
    connection_class=RequestsHttpConnection
)

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

def parse_possible_dict(val):
    """Convert Python-like dict/list strings into real objects."""
    if pd.isna(val):
        return None
    if isinstance(val, (dict, list)):
        return val
    val = str(val).strip()
    if val.startswith(("{", "[")):
        try:
            parsed = ast.literal_eval(val)
            if isinstance(parsed, (dict, list)):
                return parsed
        except Exception as e:
            print(f"⚠️ Failed to parse value: {val[:100]} ({e})")
    return val

for index in indexes:
    csv_path = f"{index}.csv"  # or s3://... if stored on S3
    print(f"Importing {csv_path} into {index}")

    try:
        # Read CSV
        df = pd.read_csv(csv_path, on_bad_lines='skip', quoting=csv.QUOTE_NONE)
        df = df.replace({float('nan'): None})

        if df.empty:
            print(f"⚠️ File {csv_path} is empty, skipping.")
            continue

        # Automatically detect and convert columns containing object-like strings
        for col in df.columns:
            sample = df[col].dropna().astype(str).head(20)
            if sample.str.startswith(("{", "[")).any():
                df[col] = df[col].apply(parse_possible_dict)

        # Write documents into destination OpenSearch
        wr.opensearch.index_documents(
            client=client,
            index=index,
            documents=df.to_dict(orient="records"),
            refresh=True,
        )

        print(f"✅ Imported {len(df)} documents into '{index}'")

    except Exception as e:
        print(f"❌ Failed to import '{index}': {e}")
