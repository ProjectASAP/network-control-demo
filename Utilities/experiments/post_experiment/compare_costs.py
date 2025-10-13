import os
import sys
import json
import argparse
import humanize
import numpy as np
import matplotlib.pyplot as plt
from typing import List
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import constants  # noqa: E402

RESOURCES = ["cpu_percent", "memory_info"]

relevant_stats = {
    "sum": lambda x: sum(x),
    "max": lambda x: max(x),
    "p95": lambda x: np.percentile(x, 95),
    "p99": lambda x: np.percentile(x, 99),
}


def pretty_print(key, value):
    if "memory" in key:
        print(key, humanize.naturalsize(value))
    else:
        print(key, round(value, 2))


def plot_resource_usage(monitor_info, experiment_mode, args):
    """
    Plot raw resource usage data for each resource type.

    Args:
        monitor_info: Dictionary with PIDs as keys and monitoring data as values
        experiment_mode: Name of the current experiment mode
        args: Command-line arguments
    """
    # Set global font size to 24
    plt.rcParams.update({"font.size": 22})

    for resource in RESOURCES:
        plt.figure(figsize=(20, 8))

        # Plot data for each PID/keyword
        for pid, data in monitor_info.items():
            keyword = data["keyword"]
            y_values = data[resource]
            x_values = list(range(len(y_values)))

            plt.plot(x_values, y_values, label=f"{keyword} (PID: {pid})")

        # Add labels and title
        resource_label = (
            "Memory Usage (bytes)" if resource == "memory_info" else "CPU Usage (%)"
        )
        plt.ylabel(resource_label)
        plt.xlabel("Time (samples)")
        plt.title(f"{experiment_mode}: {resource} Raw Data")
        plt.legend()

        # Save or show based on args
        if args.save:
            # Make plot fullscreen before saving
            mng = plt.get_current_fig_manager()
            try:
                mng.full_screen_toggle()  # For Qt backend
            except AttributeError:
                try:
                    mng.window.showMaximized()  # For TkAgg backend
                except Exception:
                    try:
                        mng.frame.Maximize(True)  # For WX backend
                    except Exception:
                        try:
                            mng.resize(*mng.window.maxsize())  # For other backends
                        except Exception:
                            print("Warning: Could not maximize figure window")

            output_filename = f"mode_{experiment_mode}_{resource}.png"
            output_path = os.path.join(args.output_dir, output_filename)
            os.makedirs(args.output_dir, exist_ok=True)
            plt.savefig(output_path, dpi=300, bbox_inches="tight")
            print(f"Saved plot to {output_path}")

        if args.show:
            plt.show()
        else:
            plt.close()


def main(args):
    if not args.print and not args.plot:
        raise ValueError("Must specify either --print or --plot")
    if args.all_experiment_modes and args.experiment_mode:
        raise ValueError(
            "Cannot specify both --all_experiment_modes and --experiment_mode"
        )
    elif not args.all_experiment_modes and not args.experiment_mode:
        raise ValueError(
            "Must specify either --all_experiment_modes or --experiment_mode"
        )

    if args.plot:
        if not args.show and not args.save:
            raise ValueError("Must specify either --show or --save when using --plot")
        if args.save:
            if not args.save_to_experiment_dir and not args.output_dir:
                raise ValueError(
                    "Must specify --save_to_experiment_dir or --output_dir when using --save"
                )

    experiment_dir = os.path.join(constants.LOCAL_EXPERIMENT_DIR, args.experiment_name)

    if args.plot and args.save:
        if args.save_to_experiment_dir:
            args.output_dir = experiment_dir

    experiment_modes: List[str] = []
    if args.all_experiment_modes:
        # experiment_modes = os.listdir(experiment_dir)
        # experiment_modes = [
        #     mode
        #     for mode in experiment_modes
        #     if os.path.exists(os.path.join(experiment_dir, mode, "monitor_output.json"))
        # ]

        # find all directories in experiment_dir recursively that contain monitor_output.json
        experiment_modes = [
            d
            for d, _, files in os.walk(
                os.path.join(experiment_dir, "remote_monitor_output")
            )
            if "monitor_output.json" in files
        ]
        # for each directory, experiment mode is the entire path relative to experiment_dir
        experiment_modes = [
            os.path.relpath(d, experiment_dir) for d in experiment_modes
        ]
        # remove the experiment_dir prefix
        experiment_modes = [
            mode for mode in experiment_modes if mode != "." and mode != ""
        ]
    else:
        experiment_modes = [args.experiment_mode]

    experiment_mode_to_overall_resource_usage = {}

    for experiment_mode in experiment_modes:
        print("-" * 20 + f" Mode: {experiment_mode} " + "-" * 20)
        monitor_info_file = os.path.join(
            experiment_dir,
            experiment_mode,
            "remote_monitor_output",
            "monitor_output.json",
        )

        monitor_info = None
        with open(monitor_info_file, "r") as f:
            monitor_info = json.load(f)

        resources_across_pids = defaultdict(list)

        pids = list(monitor_info.keys())
        # verify that all pids have the same length
        assert all(
            len(monitor_info[pid]["cpu_percent"])
            == len(monitor_info[pids[0]]["cpu_percent"])
            for pid in pids
        ), "All PIDs must have the same length of CPU percent data"
        assert all(
            len(monitor_info[pid]["memory_info"])
            == len(monitor_info[pids[0]]["memory_info"])
            for pid in pids
        ), "All PIDs must have the same length of memory info data"

        # for pid in pids:
        #     for resource in RESOURCES:
        #         monitor_info[pid][resource] = monitor_info[pid][resource][610:]

        monitor_info["all"] = {
            "keyword": "all",
            "cpu_percent": [
                0 for _ in range(len(monitor_info[pids[0]]["cpu_percent"]))
            ],
            "memory_info": [
                0 for _ in range(len(monitor_info[pids[0]]["memory_info"]))
            ],
        }

        # Add the CPU and memory data to the "all" entry
        for pid in pids:
            for i in range(len(monitor_info[pid]["cpu_percent"])):
                monitor_info["all"]["cpu_percent"][i] += monitor_info[pid][
                    "cpu_percent"
                ][i]
                monitor_info["all"]["memory_info"][i] += monitor_info[pid][
                    "memory_info"
                ][i]

        experiment_mode_to_overall_resource_usage[experiment_mode] = monitor_info["all"]

        for pid in monitor_info.keys():
            keyword = monitor_info[pid]["keyword"]
            print(pid, keyword)

            for resource in RESOURCES:
                resources_across_pids[resource].extend(monitor_info[pid][resource])

                if args.print:
                    for stat, agg_func in relevant_stats.items():
                        pretty_print(
                            f"{experiment_mode} {keyword} {resource} {stat}",
                            agg_func(monitor_info[pid][resource]),
                        )

        # if args.print:
        #     for resource, values in resources_across_pids.items():
        #         for stat, agg_func in relevant_stats.items():
        #             pretty_print(f"Overall {resource} {stat}", agg_func(values))

        # Generate plots if requested
        if args.plot:
            plot_resource_usage(monitor_info, experiment_mode, args)

    if (
        "prometheus" in experiment_mode_to_overall_resource_usage
        and "sketchdb" in experiment_mode_to_overall_resource_usage
        and args.print
    ):
        for resource in RESOURCES:
            for stat, agg_func in relevant_stats.items():
                # divide prometheus agg_func(resource) by sketchdb agg_func(resource)
                prometheus_value = agg_func(
                    experiment_mode_to_overall_resource_usage["prometheus"][resource]
                )
                sketchdb_value = agg_func(
                    experiment_mode_to_overall_resource_usage["sketchdb"][resource]
                )
                print(
                    "Benefit for {}({}): {}".format(
                        stat, resource, prometheus_value / sketchdb_value
                    )
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment_name", type=str, required=True)
    parser.add_argument("--all_experiment_modes", action="store_true")
    parser.add_argument("--experiment_mode", type=str, required=False)
    parser.add_argument("--print", action="store_true", help="Print the results")
    parser.add_argument("--plot", action="store_true", help="Plot the results")
    parser.add_argument(
        "--save", action="store_true", help="Save the results to a file"
    )
    parser.add_argument(
        "--show", action="store_true", help="Show the results on the screen"
    )
    parser.add_argument(
        "--output_dir", type=str, required=False, help="Directory to save the output"
    )
    parser.add_argument(
        "--save_to_experiment_dir",
        action="store_true",
        help="Save to experiment directory",
    )
    args = parser.parse_args()
    main(args)
