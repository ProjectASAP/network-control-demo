# Local setup
* Create a local directory, let's call it $LOCAL_DIR
* In $LOCAL_DIR, git clone FlinkSketch, QueryEngine, Utilities, PrometheusClient, Controller, prometheus-kafka-adapter
    * These are needed since this is all rsync-ed to cloudlab
* In $LOCAL_DIR/Controller, install pip dependencies from requirements.txt
* In $LOCAL_DIR/Utilities/execution/promql_utilties, run ```pip install -e .``` to install this package as a local pip package

# Cloudlab setup
* Set up a Cloudlab experiment with N nodes
* In $LOCAL_DIR/Utilities/cloudlab_setup/multi_node.sh, run ```./oneshot_setup.sh N <username> <hostname_suffix>```
    * <username> is the Cloudlab username.
    * <hostname_suffix> is the part after  “@” in the cloudlab node URL
    * Example “sketchdb.cloudmigration-PG0.utah.cloudlab.us”
    * rsync after first setup: ```./oneshot_only_rsync N <username> <hostname_suffix>```
* In $LOCAL_DIR/Utilities/installation, run ```./oneshot_setup.sh N <username> <hostname_suffix>```

# Running an end to end experiment with SketchDB
* Modify the Utilities/experiments/experiment_configs/test_config.yml if needed
* In $LOCAL_DIR/Utilities/experiments, run
    ```
    python3 experiment_run_e2e.py \
        --num_nodes N-1 \
        --cloudlab_username <username> \
        --hostname_suffix <hostname_suffix> \
        --experiment_config <path_to_above_yaml_file> \
        --experiment_name <give_a_unique_name>
    ```
    * NOTE: this will run the generate_prometheus_config.py script and the Controller locally. Everything else runs on Cloudlab. Data is automatically rsync-ed from Cloudlab back to local.
