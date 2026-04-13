"""Run the network controller solver using JSON inputs stored in the data directory."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from network_controller import (  # noqa: E402  # type: ignore  # isort: skip
    NetworkControllerSolver,
    load_edges,
    load_existing_assignments,
    load_nodes,
    load_previous_assignments,
    load_tasks,
)


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Solve the network controller ILP instance."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=PROJECT_ROOT / "data",
        help="Directory containing nodes.json, edges.json, tasks.json, etc.",
    )
    parser.add_argument(
        "--export-ilp",
        type=Path,
        help="Optional path where the generated ILP (LP format) should be saved.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = args.data_dir

    nodes = load_nodes(data_dir / "nodes.json")
    edges = load_edges(data_dir / "edges.json")
    tasks = load_tasks(data_dir / "tasks.json")

    existing_assignments_path = data_dir / "existing_assignments.json"
    existing_assignments = (
        load_existing_assignments(existing_assignments_path)
        if existing_assignments_path.exists()
        else []
    )

    previous_assignments_path = data_dir / "previous_assignments.json"
    previous_assignments = (
        load_previous_assignments(previous_assignments_path)
        if previous_assignments_path.exists()
        else {}
    )

    config_path = data_dir / "solver_config.json"
    config = _load_json(config_path) if config_path.exists() else {}

    solver = NetworkControllerSolver(nodes, edges)
    result = solver.solve(
        tasks,
        existing_assignments=existing_assignments,
        previous_assignments=previous_assignments,
        max_task_movements=config.get("max_task_movements"),
        ilp_output_path=args.export_ilp,
    )

    print("Objective value:", result.objective_value)

    if args.export_ilp:
        print(f"ILP formulation written to: {args.export_ilp}")

    print("Assignments:")
    for decision in result.decisions.values():
        print(f"  Task {decision.task_id} -> Node {decision.node_id}")
        if decision.communication_paths:
            for comm in decision.communication_paths:
                print(
                    "    Communication "
                    f"{comm.source_task_id} -> {comm.target_task_id} "
                    f"via {comm.path} (bandwidth={comm.bandwidth})"
                )
        else:
            print("    Communications: none")

    if result.unassigned_tasks:
        print("Unassigned tasks:", ", ".join(result.unassigned_tasks))
    else:
        print("Unassigned tasks: none")

    print("Moves used:", result.moves_used)
    if result.moved_tasks:
        print("Moved tasks:", ", ".join(result.moved_tasks))
    else:
        print("Moved tasks: none")


if __name__ == "__main__":
    main()
