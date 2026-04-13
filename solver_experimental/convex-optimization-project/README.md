# Convex Optimization Prototype

Experimental CVXPY-based task placement solver for the network control demo.

## Purpose in This Repo

This directory explores a convex-optimization formulation of the same epoch scheduling
problem used across the demo:

- tasks arrive each epoch
- tasks consume node CPU/memory
- communicating tasks consume inter-node bandwidth
- assignments should satisfy capacity constraints

Compared with the production benchmark path (`python_solver/` OR-Tools and
`scheduler/` PuLP), this module is exploratory and may be incomplete for all scenarios.

## Layout

- `src/main.py`: entry point
- `src/optimizer.py`: optimization model assembly and solve flow
- `src/decision_variables.py`: CVXPY decision variables for assignment
- `src/constraints/capacity_constraints.py`: node/link capacity constraints
- `src/inputs/`: loaders and typed input models

## Quick Run

From this directory:

```bash
pip install -r requirements.txt
python src/main.py
```

## Input Expectations

Input data models represent:

- tasks (resource demand, communication)
- resources/nodes (capacity and utilization)
- network topology and bandwidth
- current allocation state

Use this module as a research prototype, not as the default benchmark solver.