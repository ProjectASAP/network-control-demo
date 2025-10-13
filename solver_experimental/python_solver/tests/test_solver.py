from __future__ import annotations

from network_controller import NetworkControllerSolver, Task, build_edges, build_nodes


def test_solver_assigns_single_task():
    nodes = build_nodes({
        "node-1": {"cpu": 8, "memory": 32},
    })
    edges = build_edges({})

    task = Task(
        task_id="t0",
        cpu=2,
        memory=4,
        bandwidth=0,
        priority=1.0,
        allowed_nodes=("node-1",),
    )

    solver = NetworkControllerSolver(nodes, edges)
    result = solver.solve([task])

    assert result.moves_used == 0
    assert result.assigned_tasks() == ["t0"]
    decision = result.decisions["t0"]
    assert decision.node_id == "node-1"
    assert decision.communication_paths == ()
