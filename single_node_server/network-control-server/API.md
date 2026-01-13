# Network Control Server API

Base URL: `http://localhost:10101`

All requests are logged to stderr with headers and body.

## GET /

Returns a JSON help message with usage examples.

## GET /healthz

Returns `ok`.

## POST /cluster-metrics/_search

Elasticsearch-style search body. The server will compute some aggregations locally
and forward the remainder to the upstream URL (`UPSTREAM_URL`, default
`http://localhost:9200/cluster-metrics/_search`).

### Locally handled aggregations

An aggregation is handled locally only if it contains exactly one of the
recognized types and no extra fields.

#### percentiles

- Only fields listed under `supported_aggs.percentiles.fields` in `agg-config.yaml`
  are handled locally.
- `percents` values must be within 0..=100.
- Optional `key` is supported and must be non-empty if provided.
- `key` may be `cluster`, `task`, or `cluster;task` (the ingester stores all
  combinations).

Example:
```json
{
  "aggs": {
    "cpu_quantiles": {
      "percentiles": {
        "field": "cpu_cores",
        "percents": [10, 50]
      }
    }
  }
}
```

#### top_entities

- Only fields listed under `supported_aggs.top_entities.metrics` in `agg-config.yaml`
  are handled locally.
- Keys are tracked for `cluster`, `task`, and `cluster;task`. The returned `key`
  can be any of those.
- Values are rounded to the nearest positive integer before tracking.

Example:
```json
{
  "aggs": {
    "top_cpu": {
      "top_entities": { "field": "cpu_cores" }
    }
  }
}
```

#### cumulative

- Only fields listed under `supported_aggs.cumulative.metrics` in `agg-config.yaml`
  are handled locally.
- `key` must be non-empty.
- `key` may be `cluster`, `task`, or `cluster;task`.
- Values are rounded to the nearest positive integer before summing.

Example:
```json
{
  "aggs": {
    "cpu_sum": {
      "cumulative": { "field": "cpu_cores", "key": "cluster-a;task-1" }
    }
  }
}
```

#### frequency

- `field` must be a supported metric field.
- `key` must be non-empty.
- `value` is rounded to the nearest positive integer for counting.
- `key` may be `cluster`, `task`, or `cluster;task` (tracked via Hydra CMS).

Example:
```json
{
  "aggs": {
    "cpu_frequency": {
      "frequency": { "field": "cpu_cores", "key": "cluster-c;cache", "value": 4 }
    }
  }
}
```

Response aggregation value:
```json
{ "key": "cluster-c;cache", "value": 4, "count": 123 }
```

### Forwarded aggregations

Any other aggregation types or any aggregation with
extra fields are forwarded to the upstream URL. The response is merged with
any locally handled aggregations.

## POST /metrics/:field

Simple percentile query for a single field.

### Path parameter

`:field` supports:
- `cpu_cores` (also accepts `cpucores`, `cpu-cores`)
- `memory_gb` (also accepts `memorygb`, `memory-gb`)
- `network_mbps` (also accepts `networkmbps`, `network-mbps`)

### Body

```json
{ "quantiles": ["p10", "p20", "p50"] }
```

- `quantiles` must be a non-empty list.
- Each entry may be `pNN`, `PNN`, or a raw number (`"10"`). The server will
  normalize keys to `p{percent}` in the response.
- If a percentile cannot be computed, it is omitted from the response.

### Response

```json
{
  "field": "cpu_cores",
  "quantiles": {
    "p10": 1.23,
    "p20": 2.34
  }
}
```
