# Single-Node Server Support Matrix

## Aggregations

| Feature | Configurable | Implemented | Local `_search` | `_batch` | Notes |
|---|---|---:|---:|---:|---|
| `percentiles` | yes | yes | yes | yes | key from query term filter |
| `sum` | yes | yes | yes | yes | key from query term filter |
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
| `POST /:index/_search` | primary | index-aware facade for local + fallback search |
| `POST /:index/_batch` | primary | explicit keyed query API per index |
| `POST /metrics/:field` | deprecated | compatibility-only, default index |
| `POST /:index/metrics/:field` | deprecated | compatibility-only, explicit index |
| `POST /` | active | ingest path for default index |
| `POST /:index` | active | ingest path for explicit index |
| `GET /healthz` | active | includes config/upstream summary |
