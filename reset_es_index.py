import json
import os
import sys
import urllib.request

# --- CONFIGURATION ---
INDEX_NAME = "cluster-metrics"
ES_HOST = "http://localhost:9200"
API_KEY = "UnI2NFA1d0JkMjVjRVA5bDBsQmU6Tkd6Y3pUajVPWjJhU2ZXQWdYUGpFdw=="
# ---------------------


def es_request(method, endpoint, data=None):
    """Helper to send HTTP requests to Elasticsearch."""
    url = f"{ES_HOST}/{endpoint}"
    headers = {
        "Authorization": f"ApiKey {API_KEY}",
        "Content-Type": "application/json",
    }
    encoded_data = json.dumps(data).encode("utf-8") if data else None

    try:
        req = urllib.request.Request(
            url, data=encoded_data, headers=headers, method=method
        )
        with urllib.request.urlopen(req) as handle:
            return json.load(handle)
    except urllib.error.HTTPError as exc:
        if method == "DELETE" and exc.code == 404:
            return None
        print(f"\n[ERROR] {method} {endpoint} failed: {exc.code} {exc.reason}")
        print(exc.read().decode())
        sys.exit(1)


print(f"1. Deleting index '{INDEX_NAME}'...", end=" ")
es_request("DELETE", INDEX_NAME)
print("Done.")

print("2. Creating empty index with timestamp mapping...", end=" ")
mapping_body = {
    "mappings": {
        "properties": {
            "cpu_cores": {"type": "float"},
            "memory_gb": {"type": "float"},
            "network_mbps": {"type": "float"},
            "epoch": {"type": "integer"},
            "cluster": {"type": "keyword"},
            "task": {"type": "keyword"},
        }
    }
}
es_request("PUT", INDEX_NAME, mapping_body)
print("Done.")
