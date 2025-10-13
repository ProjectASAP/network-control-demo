# Network Controller Solver

Branch-and-bound solver that assigns network tasks to compute nodes while respecting CPU, memory, and link bandwidth capacities. The solver also lets you bound the number of task migrations between scheduling epochs.

## Project Layout

- `src/network_controller/` – Python package with the solver and JSON loading helpers.
- `data/` – Example input files consumed by the solver.
- `examples/` – Scripts showing how to run the solver against the data files.
- `tests/` – Unit tests for the core solver logic.

## Quick Start

```bash
python -m examples.run_from_files
```

Outputs include the objective value, chosen placements, unassigned tasks, and total migrations.

## Input File Formats

All inputs are JSON files located in `data/` by default:

- `nodes.json`: list of nodes with CPU/memory capacity and optional utilisation.
- `edges.json`: list of bidirectional links with capacity and optional utilisation.
- `tasks.json`: tasks describing resource demands, priorities, peer communications, and optional node eligibility constraints.
- `existing_assignments.json`: workloads already placed in the network.
- `previous_assignments.json`: previous epoch placements used for migration accounting.
- `solver_config.json`: optional settings such as `max_task_movements`.

## Using the Package

```python
from network_controller import (
    NetworkControllerSolver,
    load_edges,
    load_existing_assignments,
    load_nodes,
    load_previous_assignments,
    load_tasks,
)

nodes = load_nodes("data/nodes.json")
edges = load_edges("data/edges.json")
tasks = load_tasks("data/tasks.json")
existing = load_existing_assignments("data/existing_assignments.json")
previous = load_previous_assignments("data/previous_assignments.json")

solver = NetworkControllerSolver(nodes, edges)
result = solver.solve(
    tasks,
    existing_assignments=existing,
    previous_assignments=previous,
    max_task_movements=1,
)
```

## Testing

Install pytest (e.g. `pip install pytest`) and run:

```bash
PYTHONPATH=src pytest
```
