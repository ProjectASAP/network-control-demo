# Single-Node Server Support Matrix

## Aggregations

| Feature | Configurable | Implemented | Local `_search` | `_batch` | Notes |
|---|---|---:|---:|---:|---|
| `percentiles` | yes | yes | yes | yes | key from query term filter or agg `key` |
| `cumulative` | yes | yes | yes | yes | key from query term filter or agg `key` |
| `top_entities` | no | no | no | no | explicitly unsupported |
| `frequency` | no | no | no | no | explicitly unsupported |

## Query Shape

| Request feature | Local support | Behavior when unsupported |
|---|---:|---|
| `size: 0` | yes | `400` in strict mode, otherwise fallback upstream |
| `query.bool.filter.term` on configured key field | yes | n/a |
| `query.bool.filter.term` on `epoch` | yes | n/a |
| other query/filter clauses | no | `400` in strict mode, otherwise fallback upstream |
| extra top-level search fields | no | forwarded when fallback is enabled |

## Endpoints

| Endpoint | Status | Notes |
|---|---|---|
| `POST /cluster-metrics/_search` | primary | compatibility facade |
| `POST /cluster-metrics/_batch` | primary | explicit keyed query API |
| `POST /metrics/:field` | deprecated | compatibility-only |
| `POST /` | active | ingest path |
| `GET /healthz` | active | includes config/upstream summary |
