curl -s -X POST http://localhost:10101/metrics/cpu_cores \
  -H 'Content-Type: application/json' \
  -d '{"quantiles": ["p50","p90"], "node_id": "N001"}'