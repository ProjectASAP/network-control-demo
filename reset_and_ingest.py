import csv
import json
import os
import urllib.request
import sys

# --- CONFIGURATION ---
CSV_FILE = '~/cluster-metrics.csv'
INDEX_NAME = 'cluster-metrics'
ES_HOST = 'http://localhost:9200'
API_KEY = 'TWg0S01wc0JhR1AxOFVUcUY5N2w6bGR0TjIySHRZTHVwdmZLTmtqcGtGQQ=='
BATCH_SIZE = 5000                   
# ---------------------

def es_request(method, endpoint, data=None):
    """Helper to send HTTP requests to Elasticsearch"""
    url = f"{ES_HOST}/{endpoint}"
    headers = {
        "Authorization": f"ApiKey {API_KEY}",
        "Content-Type": "application/json"
    }
    
    if endpoint.endswith('_bulk'):
        headers["Content-Type"] = "application/x-ndjson"
        encoded_data = data.encode('utf-8')
    elif data:
        encoded_data = json.dumps(data).encode('utf-8')
    else:
        encoded_data = None

    try:
        req = urllib.request.Request(url, data=encoded_data, headers=headers, method=method)
        with urllib.request.urlopen(req) as f:
            return json.load(f)
    except urllib.error.HTTPError as e:
        if method == "DELETE" and e.code == 404:
            return None
        print(f"\n[ERROR] {method} {endpoint} failed: {e.code} {e.reason}")
        print(e.read().decode())
        sys.exit(1)

# --- STEP 1: DELETE EXISTING INDEX ---
print(f"1. Deleting index '{INDEX_NAME}'...", end=" ")
es_request("DELETE", INDEX_NAME)
print("Done.")

# --- STEP 2: CREATE INDEX WITH MAPPING ---
print(f"2. Creating index with timestamp mapping...", end=" ")
mapping_body = {
  "mappings": {
    "properties": {
      "timestamp": {
        "type": "date",
        # Added common ISO formats + your specific space-separated format
        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss||strict_date_optional_time||epoch_millis",
        "ignore_malformed": True 
      },
      "cpu_cores": { "type": "float" },
      "memory_gb": { "type": "float" },
      "network_mbps": { "type": "float" },
      "cluster": { "type": "keyword" },
      "task": { "type": "keyword" }
    }
  }
}
es_request("PUT", INDEX_NAME, mapping_body)
print("Done.")

# --- STEP 3: INGEST CSV ---
print(f"3. Ingesting data from {CSV_FILE}...")

def send_bulk(batch_data):
    if not batch_data:
        return
    bulk_body = ""
    for doc in batch_data:
        bulk_body += json.dumps({"index": {}}) + "\n"
        bulk_body += json.dumps(doc) + "\n"
    
    resp = es_request("POST", f"{INDEX_NAME}/_bulk", bulk_body)
    if resp and resp.get('errors'):
        print("\n[WARNING] Batch had errors! First error:")
        for item in resp['items']:
            if 'error' in item['index']:
                print(item['index']['error'])
                break

with open(os.path.expanduser(CSV_FILE), 'r') as f:
    reader = csv.DictReader(f)
    print(f"Detected Headers: {reader.fieldnames}")
    
    batch = []
    count = 0
    
    for row in reader:
        try:
            # Clean up data types
            if row.get('cpu_cores'): row['cpu_cores'] = float(row['cpu_cores'])
            if row.get('memory_gb'): row['memory_gb'] = float(row['memory_gb'])
            if row.get('network_mbps'): row['network_mbps'] = float(row['network_mbps'])
            
            # Handle timestamp
            if not row.get('timestamp') or not str(row['timestamp']).strip():
                if 'timestamp' in row: del row['timestamp']
            else:
                row['timestamp'] = row['timestamp'].strip()
                
        except ValueError:
            continue 

        batch.append(row)
        
        # FIX: Actually CALL the function and update the counter
        if len(batch) >= BATCH_SIZE:
            send_bulk(batch)
            count += len(batch)
            print(f"   Indexed {count} rows...", end='\r')
            batch = []

    # FIX: "Final Flush" for any remaining rows (the last partial batch)
    if batch:
        send_bulk(batch)
        count += len(batch)

print(f"\n\n✅ Success! Total documents indexed: {count}")
