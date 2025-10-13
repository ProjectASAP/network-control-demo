# Convex Optimization Project

This project implements a convex optimization program using the `cvxpy` library to optimize task allocation based on various inputs such as tasks, resources, bandwidth needs, graph topology, and current task allocation.

## Overview

The goal of this project is to efficiently allocate tasks to resources while respecting the constraints of resource capacities and bandwidth limitations. The program takes into account the current state of task allocation and aims to improve it through optimization.

## Project Structure

- `src/main.py`: Entry point for the program. Initializes the optimizer, loads input data, and executes the optimization process.
- `src/optimizer.py`: Contains the `Optimizer` class responsible for setting up and solving the convex optimization problem.
- `src/inputs/`: Directory containing modules for managing input data:
  - `tasks.py`: Defines the `Task` class for individual tasks and their resource requirements.
  - `resources.py`: Defines the `Resource` class for available physical resources.
  - `bandwidth.py`: Defines the `Bandwidth` class for bandwidth needs and capacities.
  - `topology.py`: Defines the `Topology` class for the graph structure of nodes and edges.
  - `allocation.py`: Defines the `Allocation` class for managing current task allocations.
- `src/constraints/capacity_constraints.py`: Functions to enforce capacity constraints for resources and bandwidth.
- `src/decision_variables.py`: Defines decision variables for task allocation using `cvxpy`.

## Requirements

To run this project, you need to install the required dependencies. You can do this by running:

```
pip install -r requirements.txt
```

## Running the Program

To execute the optimization program, run the following command:

```
python src/main.py
```

Make sure that all input files are properly configured and located in the appropriate directories.

## Input Files

The project expects input files to be structured according to the classes defined in the `src/inputs/` directory. Ensure that the data is formatted correctly for successful execution.

## License

This project is licensed under the MIT License.