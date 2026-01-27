import os


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() not in ("0", "false", "no")


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


SKETCH_URL = (
    os.getenv("SKETCH_URL")
    or os.getenv("SKETCH_SERVER_URL")
    or "http://localhost:10101"
)
ES_URL = os.getenv("ES_URL") or os.getenv("ES_BACKEND_URL") or "http://localhost:9200"
ES_INDEX_NAME = os.getenv("ES_INDEX_NAME", "cluster-metrics")

ES_API_KEY = os.getenv(
    "ES_API_KEY", "TWg0S01wc0JhR1AxOFVUcUY5N2w6bGR0TjIySHRZTHVwdmZLTmtqcGtGQQ=="
)
SKETCH_API_KEY = os.getenv("SKETCH_API_KEY") or ES_API_KEY
CLUSTER_METRICS_CSV = os.path.expanduser(
    os.getenv("CLUSTER_METRICS_CSV", "~/cluster-metrics.csv")
)
# Emulator ingests ES docs with "@timestamp", so default the ES time filter to match.
ES_TIME_FIELD = os.getenv("ES_TIME_FIELD", "@timestamp")
TIME_RANGE_MS = _env_int("TIME_RANGE_MS", 180_000)

QUERY_RTT_CSV = os.getenv("QUERY_RTT_CSV", "query_rtt.csv")
QUERY_COMPARE_CSV = os.getenv("QUERY_COMPARE_CSV", "query_compare.csv")
E2E_LOG_CSV = os.getenv("E2E_LOG_CSV", "e2e.csv")

PARALLEL_BENCHMARK_ENABLED = _env_bool("PARALLEL_BENCHMARK_ENABLED", True)
BENCHMARK_THREAD_POOL_SIZE = _env_int("BENCHMARK_THREAD_POOL_SIZE", 2)
CONSISTENCY_CHECK_TOLERANCE = _env_float("CONSISTENCY_CHECK_TOLERANCE", 0.01)
SYNTHETIC_NODE_ID = os.getenv("SYNTHETIC_NODE_ID", "synthetic-node")

SCHEDULER_BATCH_SIZE = _env_int("SCHEDULER_BATCH_SIZE", 5)

SKETCH_INGEST_ENABLED = _env_bool("SKETCH_INGEST_ENABLED", True)
ES_INGEST_ENABLED = _env_bool("ES_INGEST_ENABLED", True)
