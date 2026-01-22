from cvxpy import Variable, Problem, Maximize


def capacity_constraints(optimizer, resources, bandwidth, allocation):
    # Ensure resource capacity constraints are met
    resource_constraints = []
    for resource in resources:
        total_usage = sum(
            allocation[task] * resource.requirement for task in optimizer.tasks
        )
        resource_constraints.append(total_usage <= resource.capacity)

    # Ensure bandwidth capacity constraints are met
    bandwidth_constraints = []
    for edge in bandwidth.edges:
        total_bandwidth = sum(
            allocation[task] * bandwidth.requirement[task]
            for task in optimizer.tasks
            if task in edge.tasks
        )
        bandwidth_constraints.append(total_bandwidth <= edge.capacity)

    return resource_constraints + bandwidth_constraints
