import csv


class Task:
    def __init__(self, task_id, resource_requirements):
        self.task_id = task_id
        self.resource_requirements = resource_requirements

    def get_task_id(self):
        return self.task_id

    def get_resource_requirements(self):
        return self.resource_requirements

    @staticmethod
    def load_tasks(filepath):
        tasks = []
        with open(filepath, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                task_id = row["id"]
                # Remove 'id' and convert resource values to float
                resource_requirements = {
                    k: float(v) for k, v in row.items() if k != "id"
                }
                tasks.append(Task(task_id, resource_requirements))
        return tasks

    @staticmethod
    def manage_tasks(tasks):
        # Placeholder for task management logic
        pass
