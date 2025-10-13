import argparse

import utils
import constants


def main(args):
    cmd_dir = f"{constants.CLOUDLAB_HOME_DIR}/prometheus"
    cmd = "rm -rf data; rm -f queries.log"
    utils.run_on_cloudlab_node(
        0,
        args.cloudlab_username,
        args.hostname_suffix,
        cmd,
        cmd_dir,
        nohup=False,
        popen=False,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_nodes", type=int, required=True)
    parser.add_argument("--cloudlab_username", type=str, required=True)
    parser.add_argument("--hostname_suffix", type=str, required=True)
    args = parser.parse_args()
    main(args)
