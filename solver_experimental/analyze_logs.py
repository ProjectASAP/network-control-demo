import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path)


def _normalize_assignment(value: str | float | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return text
    return json.dumps(parsed, separators=(",", ":"), sort_keys=True)


def _build_round_map(e2e: pd.DataFrame) -> dict[str, int]:
    if "timestamp" in e2e.columns:
        e2e = e2e.copy()
        e2e["timestamp"] = pd.to_datetime(e2e["timestamp"], errors="coerce")
    if "metrics_source" in e2e.columns:
        base = e2e[e2e["metrics_source"] == "sketch"]
    else:
        base = e2e
    if base.empty:
        base = e2e
    base = base.dropna(subset=["correlation_id"]).drop_duplicates("correlation_id")
    if "timestamp" in base.columns:
        base = base.sort_values("timestamp")
    correlation_ids = base["correlation_id"].tolist()
    return {cid: idx + 1 for idx, cid in enumerate(correlation_ids)}


def _plot_lines(df: pd.DataFrame, x: str, y: str, group: str, title: str, out_path: Path) -> None:
    plt.figure(figsize=(10, 5))
    for name, group_df in df.groupby(group):
        group_df = group_df.sort_values(x)
        plt.plot(group_df[x], group_df[y], marker="o", label=str(name))
    plt.title(title)
    plt.xlabel(x)
    plt.ylabel(y)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def main() -> None:
    repo_root = Path(".")
    e2e_path = repo_root / "e2e.csv"
    rtt_path = repo_root / "query_rtt.csv"

    e2e = _load_csv(e2e_path)
    rtt = _load_csv(rtt_path)

    if "correlation_id" not in e2e.columns:
        raise ValueError("e2e.csv missing correlation_id column")

    e2e = e2e.dropna(subset=["correlation_id"]).copy()
    e2e["assignment_norm"] = e2e["assignment"].apply(_normalize_assignment)

    e2e_pairs = e2e[e2e["metrics_source"].isin(["sketch", "elasticsearch"])]
    e2e_pairs = e2e_pairs.sort_values("timestamp")
    e2e_pairs = e2e_pairs.drop_duplicates(["correlation_id", "metrics_source"], keep="last")
    pivot_assign = e2e_pairs.pivot(
        index="correlation_id",
        columns="metrics_source",
        values="assignment_norm",
    )
    both = pivot_assign.dropna(subset=["sketch", "elasticsearch"], how="any")
    same_count = (both["sketch"] == both["elasticsearch"]).sum()
    total_pairs = len(both)
    print(f"Same assignments (sketch vs ES): {same_count} / {total_pairs}")

    round_map = _build_round_map(e2e_pairs)
    e2e_pairs["round"] = e2e_pairs["correlation_id"].map(round_map)

    e2e_plot = e2e_pairs[e2e_pairs["metrics_source"].isin(["sketch", "elasticsearch"])]
    _plot_lines(
        e2e_plot,
        x="round",
        y="duration_ms",
        group="metrics_source",
        title="E2E Time per Round (Sketch vs ES)",
        out_path=repo_root / "e2e_comparison.png",
    )

    if "correlation_id" not in rtt.columns:
        raise ValueError("query_rtt.csv missing correlation_id column")
    if "target" not in rtt.columns:
        raise ValueError("query_rtt.csv missing target column")
    rtt = rtt.dropna(subset=["correlation_id"])
    rtt_sum = (
        rtt.groupby(["correlation_id", "target"], as_index=False)["duration_ms"]
        .sum()
        .rename(columns={"duration_ms": "total_ms"})
    )
    rtt_sum["round"] = rtt_sum["correlation_id"].map(round_map)
    rtt_sum = rtt_sum.dropna(subset=["round"])
    _plot_lines(
        rtt_sum,
        x="round",
        y="total_ms",
        group="target",
        title="Total Query RTT per Round (Sketch vs ES)",
        out_path=repo_root / "query_rtt_comparison.png",
    )

    print("Wrote plots: e2e_comparison.png, query_rtt_comparison.png")


if __name__ == "__main__":
    main()
