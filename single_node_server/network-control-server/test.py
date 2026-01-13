# this is extracted from solver to make server testing easier

import json
import time
import requests

def main():
    base_url = "http://localhost:10101"

    def get_text(path):
        url = f"{base_url}{path}"
        print("=" * 80)
        print(f"GET {url}")
        start_t = time.time()
        resp = requests.get(url)
        elapsed = time.time() - start_t
        print(f"status: {resp.status_code} ({elapsed:.4f}s)")
        try:
            print("response:")
            print(json.dumps(resp.json(), indent=2))
        except ValueError:
            print("response (non-json):")
            print(resp.text)

    def post_json(path, payload):
        url = f"{base_url}{path}"
        print("=" * 80)
        print(f"POST {url}")
        print("request:")
        print(json.dumps(payload, indent=2))
        start_t = time.time()
        resp = requests.post(url, json=payload)
        elapsed = time.time() - start_t
        print(f"status: {resp.status_code} ({elapsed:.4f}s)")
        try:
            print("response:")
            print(json.dumps(resp.json(), indent=2))
        except ValueError:
            print("response (non-json):")
            print(resp.text)

    quantiles = [10 * i for i in range(1, 10)]
    cluster = "cluster-a"
    task = "cache"
    key = f"{cluster};{task}"
    example_value = 4

    # 0) Basic endpoints
    get_text("/healthz")
    get_text("/")

    # 1) /metrics/:field (simple quantiles)
    post_json(
        "/metrics/cpu_cores",
        {"quantiles": [f"p{q}" for q in quantiles]},
    )

    # 2) /cluster-metrics/_search (percentiles, unlabeled)
    post_json(
        "/cluster-metrics/_search",
        {
            "aggs": {
                "cpu_quantiles": {
                    "percentiles": {"field": "cpu_cores", "percents": [10, 50, 90]}
                }
            }
        },
    )

    # 3) /cluster-metrics/_search (labeled percentiles)
    post_json(
        "/cluster-metrics/_search",
        {
            "aggs": {
                "cpu_quantiles_by_key": {
                    "percentiles": {
                        "field": "cpu_cores",
                        "percents": [10, 50, 90],
                        "key": key,
                    }
                }
            }
        },
    )

    # 4) /cluster-metrics/_search (frequency by label + value)
    post_json(
        "/cluster-metrics/_search",
        {
            "aggs": {
                "cpu_frequency": {
                    "frequency": {
                        "field": "cpu_cores",
                        "key": key,
                        "value": example_value,
                    }
                },
                "cpu_frequency_cluster": {
                    "frequency": {
                        "field": "cpu_cores",
                        "key": cluster,
                        "value": example_value,
                    }
                },
                "cpu_frequency_task": {
                    "frequency": {
                        "field": "cpu_cores",
                        "key": task,
                        "value": example_value,
                    }
                },
            }
        },
    )

    # 5) /cluster-metrics/_search (top_entities)
    post_json(
        "/cluster-metrics/_search",
        {
            "aggs": {
                "top_cpu": {"top_entities": {"field": "cpu_cores"}},
                "top_mem": {"top_entities": {"field": "memory_gb"}},
                "top_net": {"top_entities": {"field": "network_mbps"}},
            }
        },
    )

    # 6) /cluster-metrics/_search (cumulative by label)
    post_json(
        "/cluster-metrics/_search",
        {
            "aggs": {
                "cpu_cumulative": {
                    "cumulative": {"field": "cpu_cores", "key": key}
                },
                "cpu_cumulative_cluster": {
                    "cumulative": {"field": "cpu_cores", "key": cluster}
                },
                "cpu_cumulative_task": {
                    "cumulative": {"field": "cpu_cores", "key": task}
                },
            }
        },
    )



if __name__ == "__main__":
    main()
