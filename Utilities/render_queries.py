import yaml
from jinja2 import Environment, FileSystemLoader

CONFIG_FILE = "experiments/config/natural_query_config.yml"
JINJA_TEMPLATE = "experiments/config/experiment_type/workshop_template.j2"
OUTPUT_FILE = "experiments/config/experiment_type/workshop_demo.yaml"

def load_config(yaml_path):
    with open(yaml_path, 'r') as f:
        return yaml.safe_load(f)

def build_metric_name(metric, entity):
    entity_part = "_".join(e.strip().lower() for e in entity.split(","))
    return f"{metric.lower()}_{entity_part}"

def build_promql(metric_name, stat, window_duration):
    stat = stat.lower()
    if stat == "average":
        return f"avg_over_time({metric_name}[{window_duration}])"
    elif stat == "median":
        return f"quantile_over_time(0.5, {metric_name}[{window_duration}])"
    elif stat == "p90":
        return f"quantile_over_time(0.9, {metric_name}[{window_duration}])"
    elif stat == "p95":
        return f"quantile_over_time(0.95, {metric_name}[{window_duration}])"
    else:
        raise ValueError(f"Unknown statistic: {stat}")

def parse_duration_to_seconds(duration_str):
    """
    Convert PromQL-style duration (e.g., "5m", "30s", "1h") to seconds.
    """
    units = {'s': 1, 'm': 60, 'h': 3600}
    num = int(''.join(filter(str.isdigit, duration_str)))
    unit = ''.join(filter(str.isalpha, duration_str))
    return num * units.get(unit, 1)

def scale_duration(base_duration, multiplier):
    """
    Multiply a PromQL duration string (e.g., 5m) by an integer multiplier.
    """
    num = int(''.join(filter(str.isdigit, base_duration)))
    unit = ''.join(filter(str.isalpha, base_duration))
    return f"{num * multiplier}{unit}"

def generate_queries(config):
    queries = []

    measurement_epoch = config["epochs"]["measurement_epoch"]
    control_epoch = config["epochs"]["control_epoch"]

    for entity in config["entities"]:
        for metric in config["metrics"]:
            metric_name = build_metric_name(metric, entity)
            for stat in config["statistics"]:
                for window in config["time_windows"]:
                    for k_type, k_value in window.items():
                        if "measurement" in k_type:
                            window_duration = scale_duration(measurement_epoch, k_value)
                        elif "control" in k_type:
                            window_duration = scale_duration(control_epoch, k_value)
                        else:
                            continue
                        queries.append(build_promql(metric_name, stat, window_duration))
    return queries

def render_experiment_template(
    queries,
    measurement_epoch,
    template_dir=".",
    template_name=JINJA_TEMPLATE,
    output_file=OUTPUT_FILE
):
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template(template_name)

    repetition_delay = parse_duration_to_seconds(measurement_epoch)

    rendered = template.render(
        queries=queries,
        repetition_delay=repetition_delay
    )

    with open(output_file, "w") as f:
        f.write(rendered)

    print(f"✅ Generated {output_file} with {len(queries)} queries (repetition_delay={repetition_delay}s)")

if __name__ == "__main__":
    config = load_config(CONFIG_FILE)
    queries = generate_queries(config)
    render_experiment_template(
        queries,
        measurement_epoch=config["epochs"]["measurement_epoch"]
    )
