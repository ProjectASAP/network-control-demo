curl -s -X POST http://localhost:10101/cluster-metrics/_search \
  -H 'Content-Type: application/json' \
  -H 'x-request-type: fallback-test' \
  -d '{
    "size": 0,
    "query": {"bool": {"filter": [{"term": {"cluster": "N001"}}]}},
    "aggs": {"cpu_avg": {"avg": {"field": "cpu_cores"}}}
  }'

curl -s -X POST http://localhost:10101/cluster-metrics/_search \
  -H 'Content-Type: application/json' \
  -d '{
    "size": 0,
    "query": {"bool": {"filter": [{"term": {"cluster": "N001"}}]}},
    "aggs": {
      "cpu_p50": {"percentiles": {"field": "cpu_cores", "percents": [50]}},
      "cpu_avg": {"avg": {"field": "cpu_cores"}}
    }
  }'

curl -s -X POST http://localhost:10101/cluster-metrics/_search \
  -H 'Content-Type: application/json' \
  -d '{
    "size": 0,
    "query": {"bool": {"filter": [{"range": {"epoch": {"gte": 1}}}]}},
    "aggs": {"mem_avg": {"avg": {"field": "memory_gb"}}}
  }'
