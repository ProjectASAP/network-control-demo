import time
import uuid
import json
from dataclasses import dataclass
from loguru import logger
import requests
import os

from config import (
    ES_API_KEY,
    ES_INDEX_NAME,
    ES_URL,
    ES_TIME_FIELD,
    SKETCH_API_KEY,
    SKETCH_URL,
)
from logging_utils import log_rtt

# Environment variables for ES query config.
NODE_LABEL = os.getenv("ES_NODE_LABEL", "cluster")
TASK_LABEL = os.getenv("ES_TASK_LABEL", "task")
_NODE_METRICS_DEBUG_LOGGED = False
_NODE_METRICS_SERVER_DEBUG_LOGGED = False
_NODE_METRICS_PAYLOAD_DEBUG_LOGGED = {"es": False, "server": False}


def _log_time_window_summary(payload: dict, use_es: bool) -> None:
    # Log only the time window info so payload debug is readable.
    if use_es:
        range_block = payload.get("query", {}).get("range", {})
        if not isinstance(range_block, dict) or not range_block:
            logger.debug("ES time window: <missing range query>")
            return
        field, spec = next(iter(range_block.items()))
        if not isinstance(spec, dict):
            logger.debug("ES time window: field={} spec=<invalid>", field)
            return
        logger.debug(
            "ES time window: field={} gte={} lte={}",
            field,
            spec.get("gte"),
            spec.get("lte"),
        )
        return

    # Sketch payload time window lives inside each agg; sample the first one.
    aggs = payload.get("aggs", {})
    if not isinstance(aggs, dict) or not aggs:
        logger.debug("Sketch time window: <missing aggs>")
        return
    first_agg = next(iter(aggs.values()))
    if not isinstance(first_agg, dict) or not first_agg:
        logger.debug("Sketch time window: <invalid agg>")
        return
    agg_body = next(iter(first_agg.values()))
    if not isinstance(agg_body, dict):
        logger.debug("Sketch time window: <invalid agg body>")
        return
    logger.debug(
        "Sketch time window: current_time_ms={} time_range_ms={}",
        agg_body.get("current_time_ms"),
        agg_body.get("time_range_ms"),
    )


@dataclass
class TopEntityResult:
    # Top-entity summary for a single metric field.
    """Result from top_entities query - entity with highest value for a field."""
    field: str
    entity_key: str
    value: float


@dataclass
class CumulativeResult:
    # Aggregated sum of a task's resource usage.
    """Result from cumulative query - total resource usage for a task."""
    cpu_cores: float
    memory_gb: float
    network_mbps: float


@dataclass
class NodeMetricsSnapshot:
    # Aggregated metrics for a node.
    """Aggregated metrics for a single node."""
    node_id: str
    cpu_p25: float | None = None
    cpu_p50: float | None = None
    cpu_p75: float | None = None
    cpu_p90: float | None = None
    memory_p25: float | None = None
    memory_p50: float | None = None
    memory_p75: float | None = None
    memory_p90: float | None = None
    network_p25: float | None = None
    network_p50: float | None = None
    network_p75: float | None = None
    network_p90: float | None = None
    cumulative: CumulativeResult | None = None


def fetch_node_usage(
    session: requests.Session = None,
    node_ids: list[str] | None = None,
    use_es: bool = False,
    correlation_id: str | None = None,
    top_entities_sink: list[TopEntityResult] | None = None,
    metrics: list[str] | None = None,
    percentiles: list[int] | None = None,
    node_metrics_sink: dict[str, NodeMetricsSnapshot] | None = None,
    current_time_ms: int | None = None,
    time_range_ms: int | None = None,
    time_field: str | None = None,
) -> tuple[dict[str, NodeMetricsSnapshot], list[TopEntityResult]]:
    """
    Fetch node-level usage metrics and return node snapshots plus top entities.
    """
    if session is None:
        session = requests.Session()
    if not node_ids:
        return {}, []

    node_metrics: dict[str, NodeMetricsSnapshot] = {}
    top_entities: list[TopEntityResult] = []
    try:
        node_metrics, top_entities = get_node_metrics(
            session=session,
            node_ids=node_ids,
            use_es=use_es,
            correlation_id=correlation_id,
            metrics=metrics,
            percentiles=percentiles,
            current_time_ms=current_time_ms,
            time_range_ms=time_range_ms,
            time_field=time_field,
        )
        if top_entities_sink is not None:
            top_entities_sink.clear()
            top_entities_sink.extend(top_entities)
        if node_metrics_sink is not None:
            node_metrics_sink.clear()
            node_metrics_sink.update(node_metrics)
        for top in top_entities:
            logger.debug(f"Top entity for {top.field}: {top.entity_key} = {top.value}")
    except Exception as e:
        logger.warning(f"Failed to get node metrics: {e}")

    return node_metrics, top_entities


def _normalize_metrics(metrics: list[str] | None) -> list[tuple[str, str]]:
    mapping = {
        "cpu": "cpu_cores",
        "mem": "memory_gb",
        "memory": "memory_gb",
        "net": "network_mbps",
        "network": "network_mbps",
    }
    if metrics is None:
        metrics = ["cpu", "mem", "net"]
    normalized: list[tuple[str, str]] = []
    seen = set()
    for item in metrics:
        key = mapping.get(item)
        if key and key not in seen:
            normalized.append((item, key))
            seen.add(key)
    return normalized


def build_sketch_node_metrics_payload(
    node_ids: list[str],
    metrics: list[str] | None = None,
    percentiles: list[int] | None = None,
    current_time_ms: int | None = None,
    time_range_ms: int | None = None,
    time_field: str | None = None,
) -> dict:
    # Build a sketch-server payload for node metrics.
    # Simplified to only query cumulative (current usage per node).
    metric_fields = _normalize_metrics(metrics)
    aggs: dict[str, dict] = {}
    time_range = None
    if current_time_ms is not None:
        time_range = {
            "current_time_ms": int(current_time_ms),
            **({"time_field": time_field} if time_field else {}),
        }
    for node_id in node_ids:
        for metric_name, field in metric_fields:
            if field == "cpu_cores":
                # Percentiles commented out - only need cumulative for scheduling.
                # aggs[f"p50_cpu_{node_id}"] = {
                #     "percentiles": {
                #         "field": field,
                #         "percents": percentiles or [50],
                #         "key": node_id,
                #         **(time_range or {}),
                #     }
                # }
                aggs[f"cum_cpu_{node_id}"] = {
                    "cumulative": {
                        "field": field,
                        "key": node_id,
                        **(time_range or {}),
                    }
                }
            elif field == "memory_gb":
                # aggs[f"p50_mem_{node_id}"] = {
                #     "percentiles": {
                #         "field": field,
                #         "percents": percentiles or [50],
                #         "key": node_id,
                #         **(time_range or {}),
                #     }
                # }
                aggs[f"cum_mem_{node_id}"] = {
                    "cumulative": {
                        "field": field,
                        "key": node_id,
                        **(time_range or {}),
                    }
                }
            elif field == "network_mbps":
                # aggs[f"p50_net_{node_id}"] = {
                #     "percentiles": {
                #         "field": field,
                #         "percents": percentiles or [50],
                #         "key": node_id,
                #         **(time_range or {}),
                #     }
                # }
                aggs[f"cum_net_{node_id}"] = {
                    "cumulative": {
                        "field": field,
                        "key": node_id,
                        **(time_range or {}),
                    }
                }

    # Top entities commented out - not needed for scheduling.
    # if metric_fields:
    #     aggs["top_all"] = {
    #         "top_entities": {
    #             "fields": [field for _, field in metric_fields],
    #             **(time_range or {}),
    #         }
    #     }

    return {"size": 0, "aggs": aggs}


def build_es_node_metrics_payload(
    node_ids: list[str],
    metrics: list[str] | None = None,
    percentiles: list[int] | None = None,
    current_time_ms: int | None = None,
    time_range_ms: int | None = None,
    time_field: str | None = None,
) -> dict:
    # Build an ES payload for node metrics.
    # Simplified to only query cumulative (sum) for scheduling decisions.
    metric_fields = _normalize_metrics(metrics)
    filters = {node_id: {"term": {NODE_LABEL: node_id}} for node_id in node_ids}
    node_aggs: dict[str, dict] = {}
    for _, field in metric_fields:
        if field == "cpu_cores":
            # Percentiles commented out - only need cumulative for scheduling.
            # node_aggs["p50_cpu"] = {
            #     "percentiles": {"field": field, "percents": percentiles or [50]}
            # }
            node_aggs["cum_cpu"] = {"sum": {"field": field}}
        elif field == "memory_gb":
            # node_aggs["p50_mem"] = {
            #     "percentiles": {"field": field, "percents": percentiles or [50]}
            # }
            node_aggs["cum_mem"] = {"sum": {"field": field}}
        elif field == "network_mbps":
            # node_aggs["p50_net"] = {
            #     "percentiles": {"field": field, "percents": percentiles or [50]}
            # }
            node_aggs["cum_net"] = {"sum": {"field": field}}

    aggs: dict[str, dict] = {
        "nodes_metrics": {
            "filters": {"filters": filters},
            "aggs": node_aggs,
        }
    }

    # Top entities commented out - not needed for scheduling.
    # order_field = None
    # for _, field in metric_fields:
    #     if field == "cpu_cores":
    #         order_field = "max_cpu"
    #         break
    #     if field == "memory_gb":
    #         order_field = "max_mem"
    #         break
    #     if field == "network_mbps":
    #         order_field = "max_net"
    #         break
    #
    # if order_field:
    #     top_aggs: dict[str, dict] = {}
    #     for _, field in metric_fields:
    #         if field == "cpu_cores":
    #             top_aggs["max_cpu"] = {"max": {"field": field}}
    #         elif field == "memory_gb":
    #             top_aggs["max_mem"] = {"max": {"field": field}}
    #         elif field == "network_mbps":
    #             top_aggs["max_net"] = {"max": {"field": field}}
    #     aggs["top_all"] = {
    #         "terms": {"field": TASK_LABEL, "size": 1, "order": {order_field: "desc"}},
    #         "aggs": top_aggs,
    #     }

    payload: dict = {"size": 0, "aggs": aggs}
    if current_time_ms is not None:
        field = time_field or "@timestamp"
        filters: list[dict] = [
            {
                "script": {
                    "script": {
                        "lang": "painless",
                        "source": (
                            "doc['%s'].size()!=0 && doc['estimated_duration'].size()!=0 && "
                            "(doc['%s'].value.toInstant().toEpochMilli() + "
                            "(doc['estimated_duration'].value * 1000)) > params.current_time_ms"
                        )
                        % (field, field),
                        "params": {"current_time_ms": int(current_time_ms)},
                    }
                }
            }
        ]
        if time_range_ms is not None:
            start_ms = max(0, int(current_time_ms) - int(time_range_ms))
            filters.append(
                {
                    "range": {
                        field: {
                            "gte": start_ms,
                            "lte": int(current_time_ms),
                            "format": "epoch_millis",
                        }
                    }
                }
            )
        payload["query"] = {"bool": {"filter": filters}}
    return payload


def _get_agg_value(container: dict, key: str) -> float | None:
    # Extract a numeric value from an aggregation dict.
    agg = container.get(key)
    if not isinstance(agg, dict):
        return None
    value = agg.get("value")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_top_all_agg(agg: dict) -> list[TopEntityResult]:
    # Normalize different top-entity response shapes into TopEntityResult rows.
    results: list[TopEntityResult] = []
    if not isinstance(agg, dict):
        return results

    buckets = agg.get("buckets")
    if isinstance(buckets, list) and buckets:
        bucket = buckets[0]
        if any(key in bucket for key in ("max_cpu", "max_mem", "max_net")):
            entity_key = bucket.get("key", "")
            if entity_key:
                for field, agg_key in [
                    ("cpu_cores", "max_cpu"),
                    ("memory_gb", "max_mem"),
                    ("network_mbps", "max_net"),
                ]:
                    value = bucket.get(agg_key, {}).get("value")
                    if value is not None:
                        results.append(
                            TopEntityResult(
                                field=field,
                                entity_key=entity_key,
                                value=float(value),
                            )
                        )
            return results

        for item in buckets:
            field = item.get("field")
            entity_key = item.get("entity_key") or item.get("entity") or item.get("key")
            value = item.get("value") or item.get("max_value", {}).get("value")
            if field and entity_key and value is not None:
                results.append(
                    TopEntityResult(
                        field=field,
                        entity_key=str(entity_key),
                        value=float(value),
                    )
                )
        if results:
            return results

    for field in ("cpu_cores", "memory_gb", "network_mbps"):
        entry = agg.get(field)
        if isinstance(entry, dict):
            entity_key = (
                entry.get("key") or entry.get("entity_key") or entry.get("entity")
            )
            value = entry.get("value")
            if value is None:
                value = entry.get("max_value", {}).get("value")
            if entity_key and value is not None:
                results.append(
                    TopEntityResult(
                        field=field,
                        entity_key=str(entity_key),
                        value=float(value),
                    )
                )

    return results


def get_node_metrics(
    session: requests.Session = None,
    node_ids: list[str] | None = None,
    use_es: bool = False,
    correlation_id: str | None = None,
    metrics: list[str] | None = None,
    percentiles: list[int] | None = None,
    current_time_ms: int | None = None,
    time_range_ms: int | None = None,
    time_field: str | None = None,
) -> tuple[dict[str, NodeMetricsSnapshot], list[TopEntityResult]]:
    """
    Query node-level percentiles and cumulative usage for a list of nodes.
    """
    if session is None:
        session = requests.Session()
    if not node_ids:
        return {}, []
    node_ids = list(dict.fromkeys([node_id for node_id in node_ids if node_id]))

    # Select ES or sketch backend and issue the query.
    if use_es:
        payload = build_es_node_metrics_payload(
            node_ids,
            metrics=metrics,
            percentiles=percentiles,
            current_time_ms=current_time_ms,
            time_range_ms=time_range_ms,
            time_field=time_field,
        )
        if (
            (current_time_ms is not None or time_range_ms is not None)
            and not _NODE_METRICS_PAYLOAD_DEBUG_LOGGED["es"]
        ):
            _NODE_METRICS_PAYLOAD_DEBUG_LOGGED["es"] = True
            _log_time_window_summary(payload, use_es=True)
        response = send_search_request_payload(
            session=session,
            request_type="node_metrics",
            payload=payload,
            url=ES_URL,
            index_name=ES_INDEX_NAME,
            api_key=ES_API_KEY,
            correlation_id=correlation_id,
            target="es",
        )
    else:
        payload = build_sketch_node_metrics_payload(
            node_ids,
            metrics=metrics,
            percentiles=percentiles,
            current_time_ms=current_time_ms,
            time_range_ms=time_range_ms,
            time_field=time_field or ES_TIME_FIELD,
        )
        if (
            (current_time_ms is not None or time_range_ms is not None)
            and not _NODE_METRICS_PAYLOAD_DEBUG_LOGGED["server"]
        ):
            _NODE_METRICS_PAYLOAD_DEBUG_LOGGED["server"] = True
            _log_time_window_summary(payload, use_es=False)
        response = send_search_request_payload(
            session=session,
            request_type="node_metrics",
            payload=payload,
            url=SKETCH_URL,
            index_name=ES_INDEX_NAME,
            api_key=SKETCH_API_KEY,
            correlation_id=correlation_id,
            target="server",
        )

    if response is None:
        raise RuntimeError("node metrics request failed")
    global _NODE_METRICS_DEBUG_LOGGED
    global _NODE_METRICS_SERVER_DEBUG_LOGGED
    if use_es and not _NODE_METRICS_DEBUG_LOGGED:
        _NODE_METRICS_DEBUG_LOGGED = True
        logger.debug(
            "ES node_metrics response: {}", json.dumps(response, sort_keys=True)
        )
    if not use_es and not _NODE_METRICS_SERVER_DEBUG_LOGGED:
        _NODE_METRICS_SERVER_DEBUG_LOGGED = True
        logger.debug(
            "Sketch node_metrics response: {}", json.dumps(response, sort_keys=True)
        )

    snapshots: dict[str, NodeMetricsSnapshot] = {}
    aggregations = response.get("aggregations", {})
    top_entities = _parse_top_all_agg(aggregations.get("top_all", {}))

    # Parse the response format for each backend.
    if use_es:
        buckets = aggregations.get("nodes_metrics", {}).get("buckets", {})
        for node_id in node_ids:
            bucket = buckets.get(node_id, {})
            cpu_values = bucket.get("p50_cpu", {}).get("values", {})
            mem_values = bucket.get("p50_mem", {}).get("values", {})
            net_values = bucket.get("p50_net", {}).get("values", {})
            cpu_p25 = pick_percentile(cpu_values, 25.0, None)
            cpu_p50 = pick_percentile(cpu_values, 50.0, None)
            cpu_p75 = pick_percentile(cpu_values, 75.0, None)
            cpu_p90 = pick_percentile(cpu_values, 90.0, None)
            mem_p25 = pick_percentile(mem_values, 25.0, None)
            mem_p50 = pick_percentile(mem_values, 50.0, None)
            mem_p75 = pick_percentile(mem_values, 75.0, None)
            mem_p90 = pick_percentile(mem_values, 90.0, None)
            net_p25 = pick_percentile(net_values, 25.0, None)
            net_p50 = pick_percentile(net_values, 50.0, None)
            net_p75 = pick_percentile(net_values, 75.0, None)
            net_p90 = pick_percentile(net_values, 90.0, None)

            cum_cpu = _get_agg_value(bucket, "cum_cpu")
            cum_mem = _get_agg_value(bucket, "cum_mem")
            cum_net = _get_agg_value(bucket, "cum_net")
            cumulative = None
            if cum_cpu is not None or cum_mem is not None or cum_net is not None:
                cumulative = CumulativeResult(
                    cpu_cores=float(cum_cpu or 0.0),
                    memory_gb=float(cum_mem or 0.0),
                    network_mbps=float(cum_net or 0.0),
                )
            snapshots[node_id] = NodeMetricsSnapshot(
                node_id=node_id,
                cpu_p25=cpu_p25,
                cpu_p50=cpu_p50,
                cpu_p75=cpu_p75,
                cpu_p90=cpu_p90,
                memory_p25=mem_p25,
                memory_p50=mem_p50,
                memory_p75=mem_p75,
                memory_p90=mem_p90,
                network_p25=net_p25,
                network_p50=net_p50,
                network_p75=net_p75,
                network_p90=net_p90,
                cumulative=cumulative,
            )
    else:
        for node_id in node_ids:
            cpu_values = aggregations.get(f"p50_cpu_{node_id}", {}).get("values", {})
            mem_values = aggregations.get(f"p50_mem_{node_id}", {}).get("values", {})
            net_values = aggregations.get(f"p50_net_{node_id}", {}).get("values", {})
            cpu_p25 = pick_percentile(cpu_values, 25.0, None)
            cpu_p50 = pick_percentile(cpu_values, 50.0, None)
            cpu_p75 = pick_percentile(cpu_values, 75.0, None)
            cpu_p90 = pick_percentile(cpu_values, 90.0, None)
            mem_p25 = pick_percentile(mem_values, 25.0, None)
            mem_p50 = pick_percentile(mem_values, 50.0, None)
            mem_p75 = pick_percentile(mem_values, 75.0, None)
            mem_p90 = pick_percentile(mem_values, 90.0, None)
            net_p25 = pick_percentile(net_values, 25.0, None)
            net_p50 = pick_percentile(net_values, 50.0, None)
            net_p75 = pick_percentile(net_values, 75.0, None)
            net_p90 = pick_percentile(net_values, 90.0, None)

            cum_cpu = _get_agg_value(aggregations, f"cum_cpu_{node_id}")
            cum_mem = _get_agg_value(aggregations, f"cum_mem_{node_id}")
            cum_net = _get_agg_value(aggregations, f"cum_net_{node_id}")
            cumulative = None
            if cum_cpu is not None or cum_mem is not None or cum_net is not None:
                cumulative = CumulativeResult(
                    cpu_cores=float(cum_cpu or 0.0),
                    memory_gb=float(cum_mem or 0.0),
                    network_mbps=float(cum_net or 0.0),
                )
            snapshots[node_id] = NodeMetricsSnapshot(
                node_id=node_id,
                cpu_p25=cpu_p25,
                cpu_p50=cpu_p50,
                cpu_p75=cpu_p75,
                cpu_p90=cpu_p90,
                memory_p25=mem_p25,
                memory_p50=mem_p50,
                memory_p75=mem_p75,
                memory_p90=mem_p90,
                network_p25=net_p25,
                network_p50=net_p50,
                network_p75=net_p75,
                network_p90=net_p90,
                cumulative=cumulative,
            )

    return snapshots, top_entities


def send_search_request_payload(
    session: requests.Session,
    request_type: str,
    payload: dict,
    url: str,
    index_name: str,
    api_key: str,
    correlation_id: str | None = None,
    target: str = "server",
) -> dict | None:
    # Send a prebuilt search payload and record RTT.
    request_id = uuid.uuid4().hex[:8]
    endpoint = f"{url}/{index_name}/_search"
    headers = {
        "Authorization": f"ApiKey {api_key}",
        "Content-Type": "application/json",
        "X-Request-Id": request_id,
        "X-Request-Type": request_type,
    }

    start_t = time.perf_counter()
    response = None
    try:
        response = session.post(endpoint, json=payload, headers=headers)
        data = None
        if response.ok:
            data = response.json()
        duration_ms = (time.perf_counter() - start_t) * 1000.0
        log_rtt(
            request_id=request_id,
            correlation_id=correlation_id,
            request_type=request_type,
            target=target,
            duration_ms=duration_ms,
            status=response.status_code if response is not None else None,
            ok=response.ok if response is not None else False,
        )
        if not response.ok or data is None:
            status = response.status_code if response is not None else None
            body = ""
            if response is not None:
                try:
                    body = response.text
                except Exception:
                    body = "<unreadable response body>"
            logger.warning(
                "Search request failed (target={}, type={}, status={}): {}",
                target,
                request_type,
                status,
                body,
            )
            return None
        return data
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_t) * 1000.0
        log_rtt(
            request_id=request_id,
            correlation_id=correlation_id,
            request_type=request_type,
            target=target,
            duration_ms=duration_ms,
            status=response.status_code if response is not None else None,
            ok=False,
            error=str(exc),
        )
        logger.warning(
            "Search request exception (target={}, type={}): {}",
            target,
            request_type,
            exc,
        )
        return None


def send_search_request(
    session: requests.Session,
    request_type: str,
    query: dict | None,
    aggs: dict,
    url: str,
    index_name: str,
    api_key: str,
    correlation_id: str | None = None,
    target: str = "server",
) -> dict | None:
    # Build and send a sketch-server search request and record RTT.
    request_id = uuid.uuid4().hex[:8]
    payload = {
        "size": 0,
        "aggs": aggs,
    }
    if query is not None:
        payload["query"] = query

    endpoint = f"{url}/{index_name}/_search"
    headers = {
        "Authorization": f"ApiKey {api_key}",
        "Content-Type": "application/json",
        "X-Request-Id": request_id,
        "X-Request-Type": request_type,
    }

    start_t = time.perf_counter()
    response = None
    try:
        response = session.post(endpoint, json=payload, headers=headers)
        data = None
        if response.ok:
            data = response.json()
        duration_ms = (time.perf_counter() - start_t) * 1000.0
        log_rtt(
            request_id=request_id,
            correlation_id=correlation_id,
            request_type=request_type,
            target=target,
            duration_ms=duration_ms,
            status=response.status_code if response is not None else None,
            ok=response.ok if response is not None else False,
        )
        if not response.ok or data is None:
            return None
        return data
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_t) * 1000.0
        log_rtt(
            request_id=request_id,
            correlation_id=correlation_id,
            request_type=request_type,
            target=target,
            duration_ms=duration_ms,
            status=response.status_code if response is not None else None,
            ok=False,
            error=str(exc),
        )
        return None


def pick_percentile(
    values: dict, percentile: float, default: float | None
) -> float | None:
    # Resolve percentile values that may use stringified numeric keys.
    direct = values.get(str(percentile))
    if direct is not None:
        return direct
    if percentile.is_integer():
        direct_int = values.get(str(int(percentile)))
        if direct_int is not None:
            return direct_int
    alt = values.get(f"{percentile:.1f}")
    if alt is not None:
        return alt
    numeric = values.get(percentile)
    if numeric is not None:
        return numeric
    if percentile.is_integer():
        numeric_int = values.get(int(percentile))
        if numeric_int is not None:
            return numeric_int
    return default


def check_es_available(timeout_s: float = 1.0) -> bool:
    # Ping Elasticsearch to decide whether direct queries are possible.
    es_url = ES_URL.strip()
    if not es_url:
        return False
    headers = {}
    if ES_API_KEY:
        headers["Authorization"] = f"ApiKey {ES_API_KEY}"
    try:
        response = requests.get(es_url, headers=headers, timeout=timeout_s)
        return response.ok
    except Exception:
        return False


def _relative_diff(left: float, right: float) -> float:
    # Compute relative difference with a floor to avoid divide-by-zero.
    return abs(left - right) / max(abs(left), abs(right), 1e-9)


def compare_node_metrics(
    sketch_metrics: dict[str, NodeMetricsSnapshot],
    es_metrics: dict[str, NodeMetricsSnapshot],
    tolerance: float = 0.01,
) -> list[str]:
    # Compare node metric snapshots and report mismatches beyond tolerance.
    discrepancies: list[str] = []
    sketch_keys = set(sketch_metrics.keys())
    es_keys = set(es_metrics.keys())
    missing_in_es = sketch_keys - es_keys
    missing_in_sketch = es_keys - sketch_keys
    for node_id in sorted(missing_in_es):
        discrepancies.append(f"Node {node_id} missing in ES metrics")
    for node_id in sorted(missing_in_sketch):
        discrepancies.append(f"Node {node_id} missing in sketch metrics")

    for node_id in sorted(sketch_keys & es_keys):
        sketch = sketch_metrics[node_id]
        es = es_metrics[node_id]
        comparisons = [
            ("cpu_p25", sketch.cpu_p25, es.cpu_p25),
            ("cpu_p50", sketch.cpu_p50, es.cpu_p50),
            ("cpu_p75", sketch.cpu_p75, es.cpu_p75),
            ("cpu_p90", sketch.cpu_p90, es.cpu_p90),
            ("memory_p25", sketch.memory_p25, es.memory_p25),
            ("memory_p50", sketch.memory_p50, es.memory_p50),
            ("memory_p75", sketch.memory_p75, es.memory_p75),
            ("memory_p90", sketch.memory_p90, es.memory_p90),
            ("network_p25", sketch.network_p25, es.network_p25),
            ("network_p50", sketch.network_p50, es.network_p50),
            ("network_p75", sketch.network_p75, es.network_p75),
            ("network_p90", sketch.network_p90, es.network_p90),
        ]
        for label, sketch_value, es_value in comparisons:
            if sketch_value is None and es_value is None:
                continue
            if sketch_value is None or es_value is None:
                discrepancies.append(
                    f"Node {node_id} {label} mismatch: {sketch_value} vs {es_value}"
                )
                continue
            if _relative_diff(sketch_value, es_value) >= tolerance:
                discrepancies.append(
                    f"Node {node_id} {label} diff {sketch_value} vs {es_value}"
                )

        if sketch.cumulative is None and es.cumulative is None:
            continue
        if sketch.cumulative is None or es.cumulative is None:
            discrepancies.append(
                f"Node {node_id} cumulative mismatch: {sketch.cumulative} vs {es.cumulative}"
            )
        else:
            cumulative_pairs = [
                (
                    "cumulative.cpu_cores",
                    sketch.cumulative.cpu_cores,
                    es.cumulative.cpu_cores,
                ),
                (
                    "cumulative.memory_gb",
                    sketch.cumulative.memory_gb,
                    es.cumulative.memory_gb,
                ),
                (
                    "cumulative.network_mbps",
                    sketch.cumulative.network_mbps,
                    es.cumulative.network_mbps,
                ),
            ]
            for label, sketch_value, es_value in cumulative_pairs:
                if _relative_diff(sketch_value, es_value) >= tolerance:
                    discrepancies.append(
                        f"Node {node_id} {label} diff {sketch_value} vs {es_value}"
                    )

    return discrepancies
