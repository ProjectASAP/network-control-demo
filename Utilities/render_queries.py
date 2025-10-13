import yaml
from jinja2 import Environment, FileSystemLoader
from pathlib import Path

CONFIG_FILE = "experiments/config/natural_query_config.yml"
JINJA_TEMPLATE = "experiments/config/experiment_type/workshop_template.j2"
OUTPUT_FILE = "experiments/config/experiment_type/workshop_demo.yaml"

def load_config(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def build_metric_name(metric, entity):
    return f"{metric.lower()}_{entity.lower()}"

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

def parse_duration_to_seconds(duration):
    units = {"s":1, "m":60, "h":3600}
    num = int(''.join(filter(str.isdigit, duration)))
    unit = ''.join(filter(str.isalpha, duration))
    return num * units.get(unit, 1)

def scale_duration(duration, multiplier):
    num = int(''.join(filter(str.isdigit, duration)))
    unit = ''.join(filter(str.isalpha, duration))
    return f"{num * multiplier}{unit}"

# ---------- Generate queries and complementary metrics ----------
def generate_queries_and_metrics(config):
    queries = []
    complementary_metrics = []  # For Jinja template

    measurement_epoch = config["epochs"]["measurement_epoch"]
    control_epoch = config["epochs"]["control_epoch"]

    for metric_entry in config["metrics"]:
        metric = metric_entry["metric"]
        entities = metric_entry.get("entities", [])
        for entity in entities:
            metric_name = build_metric_name(metric, entity)
            # Compute complementary entities
            complementary_entities = [e for e in entities if e != entity]
            complementary_metrics.append({
                "metric": metric_name,
                "entities": complementary_entities
            })

            # Generate queries
            for stat in config["statistics"]:
                for window in config["time_windows"]:
                    for k_type, k_value in window.items():
                        if "measurement" in k_type:
                            duration = scale_duration(measurement_epoch, k_value)
                        elif "control" in k_type:
                            duration = scale_duration(control_epoch, k_value)
                        else:
                            continue
                        queries.append(build_promql(metric_name, stat, duration))

    return queries, complementary_metrics

# ---------- Render Jinja template ----------
def render_template(config, queries, complementary_metrics,
                    template_file=JINJA_TEMPLATE,
                    output_file=OUTPUT_FILE):
    env = Environment(loader=FileSystemLoader("."))
    template = env.get_template(template_file)
    repetition_delay = parse_duration_to_seconds(config["epochs"]["measurement_epoch"])
    rendered = template.render(
        queries=queries,
        repetition_delay=repetition_delay,
        metrics=complementary_metrics
    )
    with open(output_file, "w") as f:
        f.write(rendered)
    print(f"✅ Experiment config generated: {output_file} ({len(queries)} queries)")

# ---------- Main ----------
if __name__ == "__main__":
    config_path = Path(CONFIG_FILE)
    if not config_path.exists():
        raise FileNotFoundError("config.yml not found")
    config = load_config(config_path)
    queries, complementary_metrics = generate_queries_and_metrics(config)
    render_template(config, queries, complementary_metrics)


