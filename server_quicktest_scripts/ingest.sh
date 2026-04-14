curl -s -X POST http://localhost:10101/ \
  -H 'Content-Type: application/json' \
  -d '{
    "cluster": ["N001","N001","N002"],
    "task":    ["t1","t2","t1"],
    "epoch": 1,
    "cpu_cores":    [1.2, 2.5, 0.8],
    "memory_gb":    [4.0, 8.0, 2.0],
    "network_mbps": [100, 200, 50]
  }'