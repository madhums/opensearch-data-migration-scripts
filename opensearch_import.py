from opensearchpy import OpenSearch, RequestsHttpConnection
from dotenv import load_dotenv
import awswrangler as wr
from pathlib import Path
import pandas as pd
import json
import ast
import csv
import os

# --- Load environment variables from .env file if present ---
load_dotenv()

# --- Configuration ---
HOST = os.getenv("OPENSEARCH_TARGET_HOST")
USERNAME = os.getenv("OPENSEARCH_TARGET_USER")
PASSWORD = os.getenv("OPENSEARCH_TARGET_PASS")

# --- Create client ---
search_client = OpenSearch(
    hosts=[{"host": HOST, "port": 9200}],
    http_auth=(USERNAME, PASSWORD),
    use_ssl=True,          # set to True if your server uses HTTPS
    verify_certs=False,     # only needed if using self-signed SSL
    ssl_assert_hostname=False,
    ssl_show_warn=False,
    connection_class=RequestsHttpConnection
)

current_dir = Path(".")

# --- List all .csv files without extension (names of indexes) ---
indexes = [f.stem for f in current_dir.glob("*.csv") if f.is_file()]

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
            client=search_client,
            index=index,
            documents=df.to_dict(orient="records"),
            refresh=True,
        )

        print(f"✅ Imported {len(df)} documents into '{index}'")

    except Exception as e:
        print(f"❌ Failed to import '{index}': {e}")
