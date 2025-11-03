import pandas as pd
from elasticsearch import Elasticsearch, helpers

# --- CONNECT TO ELASTICSEARCH ---
client = Elasticsearch(
    "https://data-operations-it-assets-e9f74b.es.us-east-1.aws.elastic.cloud:443",
    api_key="TGNtblBwb0J1V2EzTEgzaTE0akk6NWlWRFNIX3dtQzJRQko1UVF4cGxKUQ==",
    verify_certs=False   # disable SSL verification (safe for local testing)
)
index_name = "it_assets_inventory"

# --- LOAD CSV (update path if needed) ---
df = pd.read_csv("it_asset_inventory_enriched.csv")


# --- CONVERT TO LIST OF DICTS ---
records = df.to_dict(orient="records")

# --- BULK INDEX INTO ELASTICSEARCH ---
actions = [
    {"_index": index_name, "_source": record}
    for record in records
]

helpers.bulk(client, actions)
print("Data indexed successfully!")

# --- VERIFY DATA ---
res = client.search(index=index_name, query={"match_all": {}}, size=5)
print("Sample indexed documents:")
for hit in res["hits"]["hits"]:
    print(hit["_source"])
