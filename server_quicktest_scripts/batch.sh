curl -s -X POST http://localhost:10101/cluster-metrics/_batch \
  -H 'Content-Type: application/json' \
  -d '{
    "keys": ["N001","N002"],
    "aggs": ["percentiles","cumulative"],
    "fields": ["cpu_cores","memory_gb"],
    "percents": [50, 90]
  }'