from elasticsearch import Elasticsearch, helpers
from datetime import datetime

# --- CONNECT TO ELASTICSEARCH ---
client = Elasticsearch(
    "https://data-operations-it-assets-e9f74b.es.us-east-1.aws.elastic.cloud:443",
    api_key="TGNtblBwb0J1V2EzTEgzaTE0akk6NWlWRFNIX3dtQzJRQko1UVF4cGxKUQ==",
    verify_certs=False  # disable SSL verification for local testing
)

# --- INDEX NAMES ---
SRC_INDEX = "it_assets_inventory"
DST_INDEX = "it_assets_inventory_transformed"

# --- CHECK CONNECTION ---
if not client.ping():
    print("Connection failed! Check API key or endpoint.")
    raise SystemExit(1)
print("Connected to Elasticsearch!")

# --- CHECK SOURCE INDEX ---
if not client.indices.exists(index=SRC_INDEX):
    print(f"Source index '{SRC_INDEX}' not found. Run index_data.py first.")
    raise SystemExit(1)

# === STEP 1: COPY DATA ===
print("Copying data to new index...")
client.reindex(
    body={"source": {"index": SRC_INDEX}, "dest": {"index": DST_INDEX}},
    wait_for_completion=True,
    refresh=True
)
print(f"Data copied → {DST_INDEX}")

# === STEP 2: ADD risk_level ===
print("Adding risk_level field...")
client.update_by_query(
    index=DST_INDEX,
    body={
        "script": {
            "source": """
                def st = ctx._source.get('operating_system_lifecycle_status');
                if (st == 'EOL' || st == 'EOS') ctx._source.risk_level = 'High';
                else ctx._source.risk_level = 'Low';
            """,
            "lang": "painless"
        },
        "query": {"match_all": {}}
    },
    refresh=True,
    conflicts="proceed"
)
print("risk_level added")

# === STEP 3: CALCULATE system_age ===
print("Calculating system_age (years)...")
current_year = datetime.utcnow().year
updates = []

resp = client.search(
    index=DST_INDEX,
    body={
        "size": 1000,
        "_source": ["operating_system_installation_date"],
        "query": {"exists": {"field": "operating_system_installation_date"}}
    }
)

for hit in resp["hits"]["hits"]:
    _id = hit["_id"]
    date_str = hit["_source"].get("operating_system_installation_date")
    try:
        year = int(str(date_str)[:4])
        age = max(0, current_year - year)
        updates.append({"_op_type": "update", "_index": DST_INDEX, "_id": _id, "doc": {"system_age": age}})
    except Exception:
        continue

if updates:
    helpers.bulk(client, updates)
    print(f"Added system_age for {len(updates)} records")
else:
    print("No valid installation dates found")

# === STEP 4: DELETE BAD RECORDS ===
print("Removing invalid records...")
client.delete_by_query(
    index=DST_INDEX,
    body={
        "query": {
            "bool": {
                "should": [
                    {"bool": {"must_not": {"exists": {"field": "hostname"}}}},
                    {"term": {"operating_system_provider": "Unknown"}}
                ],
                "minimum_should_match": 1
            }
        }
    },
    refresh=True,
    conflicts="proceed"
)
print("Invalid records removed")

# === STEP 5: SHOW SAMPLE ===
print("\nSample transformed records:")
sample = client.search(
    index=DST_INDEX,
    body={
        "size": 5,
        "_source": ["hostname", "operating_system_installation_date", "system_age", "risk_level"]
    }
)
for doc in sample["hits"]["hits"]:
    print(doc["_source"])

print(f"\n Transformation complete! Final index: {DST_INDEX}")
