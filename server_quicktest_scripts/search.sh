curl -s -X POST http://localhost:10101/cluster-metrics/_search \
  -H 'Content-Type: application/json' \
  -d '{
    "size": 0,
    "query": {"bool": {"filter": [{"term": {"cluster": "N001"}}]}},
    "aggs": {"cpu_p50": {"percentiles": {"field": "cpu_cores", "percents": [50]}}}
  }'

curl -s -X POST http://localhost:10101/cluster-metrics/_search \
  -H 'Content-Type: application/json' \
  -d '{
    "size": 0,
    "aggs": {"mem_sum": {"cumulative": {"field": "memory_gb", "key": "N001"}}}
  }'