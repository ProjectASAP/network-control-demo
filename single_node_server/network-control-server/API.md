# Network Control Server API

Base URL by default: `http://localhost:10101`

The runtime contract is driven by `server-config.yaml`.

## Supported Surface

- `POST /:index/_search`
- `POST /:index/_batch`
- `POST /metrics/:field` (compatibility endpoint, deprecated)
- `POST /:index/metrics/:field` (index-aware compatibility endpoint)
- `POST /` for ingest
- `POST /:index` for index-aware ingest
- `GET /healthz`

## Local Search Contract

`POST /:index/_search` (example: `POST /cluster-metrics/_search`)

Supported local aggregations:

- `percentiles`
- `sum`

Supported local query subset:

- `size: 0`
- `query.bool.filter.term` on configured key fields such as `cluster`
- `query.bool.filter.term` on `epoch`

Anything outside that subset is either:

- forwarded upstream when `upstream.mode: fallback`
- rejected with `400` when strict mode is enabled or upstream fallback is disabled

### Percentiles

```json
{
  "size": 0,
  "query": {
    "bool": {
      "filter": [
        { "term": { "cluster": "N001" } }
      ]
    }
  },
  "aggs": {
    "cpu_quantiles": {
      "percentiles": {
        "field": "cpu_cores",
        "percents": [10, 50, 90]
      }
    }
  }
}
```

The key is always derived from `query.bool.filter.term` (sketch-server-specific
`key` overrides on aggregations are no longer supported now that requests are
parsed as standard ES DSL).

### Sum

```json
{
  "size": 0,
  "query": {
    "bool": {
      "filter": [
        { "term": { "cluster": "N001" } }
      ]
    }
  },
  "aggs": {
    "cpu_sum": {
      "sum": {
        "field": "cpu_cores"
      }
    }
  }
}
```

### Error shape

```json
{
  "code": "unsupported_request",
  "message": "request contains unsupported local query features",
  "details": ["unsupported_query: only bool.filter.term queries are supported locally"],
  "supported_features": [
    "aggregations.percentiles",
    "aggregations.sum",
    "query.bool.filter.term",
    "size=0"
  ]
}
```

## Batch Contract

`POST /:index/_batch` (example: `POST /cluster-metrics/_batch`)

```json
{
  "keys": ["N001", "N002"],
  "fields": ["cpu_cores", "memory_gb"],
  "aggs": ["percentiles", "sum"],
  "percents": [50, 90]
}
```

Notes:

- `keys` and `fields` is required.
- `percents` defaults from `query_support.default_batch_percents`.
- Only configured and registered aggs are accepted.

## Metrics Compatibility Endpoint

`POST /metrics/:field` or `POST /:index/metrics/:field`

```json
{
  "quantiles": ["p50", "p90"],
  "node_id": "N001"
}
```

Response includes `"deprecated": true`.

## Ingest

`POST /` (uses the default configured index) or `POST /:index`

```json
{
  "epoch": 1,
  "task": ["T001", "T002"],
  "cluster": ["N001", "N002"],
  "cpu_cores": [2.5, 3.1],
  "memory_gb": [8.0, 16.0],
  "network_mbps": [100.0, 200.0]
}
```

## Not Supported Locally

- `top_entities`
- `frequency`
- arbitrary Elasticsearch DSL
- non-`term` filters
- non-zero `size`
