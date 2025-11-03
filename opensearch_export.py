from opensearchpy import OpenSearch
from dotenv import load_dotenv
import awswrangler as wr
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
response = search_client.cat.indices(format="json")

# --- List of indexes to export ---
indexes = [item["index"] for item in response]

# --- Export loop ---
for index in indexes:
    time.sleep(2)
    print(f"\n🚀 Exporting index: {index}")

    output_path = f"data/{index}.csv"
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
