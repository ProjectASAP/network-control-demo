import os
import json
import subprocess

import hydra
from omegaconf import DictConfig, OmegaConf

import utils
import constants
import experiment_utils
from experiment_utils.services import (
    DeathstarService,
    MonitoringService,
    PrometheusService,
)


def generate_and_copy_config(
    num_nodes_in_experiment, local_experiment_dir, streaming_config
):
    args = experiment_utils.GeneratePrometheusArgs(
        num_nodes_in_experiment,
        local_experiment_dir,
        "prometheus_config",
    )
    args.remote_write_metric_names = ["node_cpu_seconds_total"]

    # Build remote_write_url from streaming config
    remote_write_config = streaming_config["remote_write"]
    ip = remote_write_config["ip"]
    base_port = remote_write_config["base_port"]
    path = remote_write_config["path"]
    args.remote_write_url = f"http://{ip}:{base_port}{path}"

    # Use empty experiment config since this is a simple setup
    experiment_utils.call_generate_prometheus_config(args, {})


def rsync_config(username, hostname_suffix):
    hostname = f"node0.{hostname_suffix}"
    cmd = 'rsync -azh -e "ssh -o StrictHostKeyChecking=no" ./prometheus_config {}@{}:{}'.format(
        username,
        hostname,
        os.path.join(constants.CLOUDLAB_HOME_DIR, "cloudlab_scripts"),
    )
    subprocess.run(cmd, shell=True, check=True)


def export_prometheus_data(username, hostname_suffix, experiment_output_dir):
    cmd = "python3 export_prometheus_data.py --output_dir {}".format(
        os.path.join(experiment_output_dir, "exported_prometheus_data")
    )
    cmd_dir = f"{constants.CLOUDLAB_HOME_DIR}/cloudlab_scripts"
    utils.run_on_cloudlab_node(
        0,
        username,
        hostname_suffix,
        cmd,
        cmd_dir,
        nohup=False,
        popen=False,
        manual=False,
    )


def copy_prometheus_data(username, hostname_suffix, experiment_name):
    cmd = "python3 copy_prometheus_data.py --cloudlab_username {} --hostname_suffix {} --output_dir {}".format(
        username,
        hostname_suffix,
        os.path.join(
            constants.LOCAL_EXPERIMENT_DIR, experiment_name, "raw_prometheus_data"
        ),
    )
    subprocess.run(cmd, shell=True, check=True)


def rsync_experiment_data(username, hostname_suffix, experiment_name):
    cmd = f'rsync -azh -e "ssh {constants.SSH_OPTIONS}" {username}@node0.{hostname_suffix}:{constants.CLOUDLAB_HOME_DIR}/experiments/{experiment_name} {constants.LOCAL_EXPERIMENT_DIR}/'
    print(cmd)
    subprocess.run(cmd, shell=True, check=True)


def validate_config(cfg: DictConfig):
    """
    Validate configuration parameters for exporters and prometheus experiment.
    """
    # Check for required parameters that must be provided via command line
    # Note: This experiment only needs basic CloudLab params, no config files
    required_params = [
        ("experiment.name", "Human-readable experiment name"),
        ("cloudlab.num_nodes", "Number of CloudLab nodes to use"),
        ("cloudlab.username", "Your CloudLab username"),
        ("cloudlab.hostname_suffix", "CloudLab experiment hostname suffix"),
    ]

    missing_params = []
    for param_path, description in required_params:
        try:
            value = OmegaConf.select(cfg, param_path)
            if value is None or (isinstance(value, str) and value == "???"):
                missing_params.append((param_path, description))
        except Exception:
            missing_params.append((param_path, description))

    if missing_params:
        error_msg = "Required parameters must be provided via command line:\n\n"
        for param_path, description in missing_params:
            error_msg += f"  {param_path}: {description}\n"

        error_msg += "\nExample usage:\n"
        error_msg += "python experiment_run_exporters_and_prometheus.py \\\n"
        error_msg += "  experiment.name=monitoring_test \\\n"
        error_msg += "  cloudlab.num_nodes=4 \\\n"
        error_msg += "  cloudlab.username=myuser \\\n"
        error_msg += "  cloudlab.hostname_suffix=myexp.cloudlab.us\n"

        raise ValueError(error_msg)


class Args:
    """Helper class to convert Hydra config to argparse-like namespace"""

    def __init__(self, cfg: DictConfig):
        # Experiment configuration
        self.experiment_name = cfg.experiment.name

        # CloudLab configuration
        self.num_nodes = cfg.cloudlab.num_nodes
        self.cloudlab_username = cfg.cloudlab.username
        self.hostname_suffix = cfg.cloudlab.hostname_suffix


def main(args, streaming_config):
    local_experiment_dir = os.path.join(
        constants.LOCAL_EXPERIMENT_DIR, args.experiment_name
    )
    os.makedirs(local_experiment_dir, exist_ok=True)

    # Initialize services
    monitoring_service = MonitoringService(
        args.cloudlab_username, args.hostname_suffix, args.num_nodes
    )
    deathstar_service = DeathstarService(
        args.cloudlab_username, args.hostname_suffix, args.num_nodes
    )
    prometheus_service = PrometheusService(
        args.cloudlab_username, args.hostname_suffix, args.num_nodes
    )

    # Stop any existing services
    monitoring_service.stop()
    deathstar_service.stop()
    prometheus_service.reset()

    experiment_output_dir = (
        f"{constants.CLOUDLAB_HOME_DIR}/experiments/{args.experiment_name}"
    )
    utils.run_on_cloudlab_nodes_in_parallel(
        range(args.num_nodes + 1),
        args.cloudlab_username,
        args.hostname_suffix,
        f"mkdir -p {experiment_output_dir}",
        "",
        nohup=False,
        popen=True,
    )
    utils.run_on_cloudlab_node(
        0,
        args.cloudlab_username,
        args.hostname_suffix,
        "mkdir -p {}".format(os.path.dirname(constants.CLOUDLAB_QUERY_LOG_FILE)),
        "",
        nohup=False,
        popen=False,
    )

    num_nodes_in_experiment = args.num_nodes

    generate_and_copy_config(
        num_nodes_in_experiment, local_experiment_dir, streaming_config
    )
    rsync_config(args.cloudlab_username, args.hostname_suffix)

    # Start services
    deathstar_service.start()

    # Start monitoring with minimal config
    minimal_config = {
        "exporters": {
            "exporter_list": {"node_exporter": {"port": 9100, "extra_flags": ""}}
        }
    }
    monitoring_service.start(
        experiment_params=minimal_config, experiment_output_dir=experiment_output_dir
    )

    # Run workload - this blocks until the workload is done
    DURATION = 30  # Duration for this simple experiment
    deathstar_service.run_workload(
        experiment_output_dir=experiment_output_dir,
        local_experiment_dir=local_experiment_dir,
        minimum_experiment_running_time=DURATION,
        random_params=False,
    )
    # prometheus returns zero metrics if we don't wait
    input("Press enter to stop")
    # copy_prometheus_data(args.cloudlab_username, args.hostname_suffix, args.experiment_name)
    # export_prometheus_data(args.cloudlab_username, args.hostname_suffix, experiment_output_dir)

    # Stop services
    monitoring_service.stop()
    deathstar_service.stop()
    prometheus_service.reset()
    # rsync_experiment_data(args.hostname_suffix, args.experiment_name)


@hydra.main(version_base=None, config_path="config", config_name="config")
def hydra_main(cfg: DictConfig):
    # Validate configuration
    validate_config(cfg)

    # Convert config to args-like object for backward compatibility
    args = Args(cfg)

    # Create experiment output directory structure
    local_experiment_root_dir = os.path.join(
        constants.LOCAL_EXPERIMENT_DIR, args.experiment_name
    )
    os.makedirs(local_experiment_root_dir, exist_ok=True)

    # dump config to a file
    with open(os.path.join(local_experiment_root_dir, "hydra_config.yaml"), "w") as f:
        OmegaConf.save(cfg, f)

    # Also dump args to a file for backward compatibility
    with open(os.path.join(local_experiment_root_dir, "cmdline_args.txt"), "w") as f:
        json.dump(vars(args), f)

    print(f"Running exporters and prometheus experiment: {args.experiment_name}")
    main(args, cfg.streaming)


if __name__ == "__main__":
    hydra_main()
