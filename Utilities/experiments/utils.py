from typing import Union
import subprocess

import constants


def get_ssh_cmd(username, ip, cmd, cmd_dir, nohup, redirect=False):
    user = username
    if nohup:
        cmd = f"nohup {cmd}"

    if cmd_dir:
        cmd = f'ssh {constants.SSH_OPTIONS} {user}@{ip} "cd {cmd_dir}; {cmd}"'
    else:
        cmd = f'ssh {constants.SSH_OPTIONS} {user}@{ip} "{cmd}"'

    if redirect:
        cmd = f"{cmd} < /dev/null > /dev/null 2>&1"
    return cmd


def run_cmd(
    cmd, popen, ignore_errors=False
) -> Union[subprocess.Popen, subprocess.CompletedProcess]:
    print(cmd)
    if popen:
        return subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
    else:
        return subprocess.run(
            cmd,
            shell=True,
            check=not ignore_errors,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )


def run_on_cloudlab_node(
    node_idx,
    username,
    hostname_suffix,
    cmd,
    cmd_dir,
    nohup,
    popen,
    ignore_errors=False,
    manual=False,
):
    hostname = f"node{node_idx}.{hostname_suffix}"
    ssh_cmd = get_ssh_cmd(username, hostname, cmd, cmd_dir, nohup)
    if manual:
        print(f"Run the following command on {hostname}:")
        print(ssh_cmd)
        input("Press Enter to continue...")
        return
    return run_cmd(ssh_cmd, popen, ignore_errors=ignore_errors)


def run_on_cloudlab_nodes_in_parallel(
    node_idxs, username, hostname_suffix, cmd, cmd_dir, nohup, popen, redirect=False
):
    if not popen:
        raise ValueError("popen must be True to run commands in parallel")

    popens = []
    for node_idx in node_idxs:
        hostname = f"node{node_idx}.{hostname_suffix}"
        ssh_cmd = get_ssh_cmd(username, hostname, cmd, cmd_dir, nohup, redirect)
        popens.append(run_cmd(ssh_cmd, popen))

    for popen in popens:
        popen.wait()


def run_on_cloudlab_nodes_in_parallel_without_wait(
    node_idxs, username, hostname_suffix, cmd, cmd_dir, nohup, popen, redirect=False
):
    if not popen:
        raise ValueError("popen must be True to run commands in parallel")

    popens = []
    for node_idx in node_idxs:
        hostname = f"node{node_idx}.{hostname_suffix}"
        ssh_cmd = get_ssh_cmd(username, hostname, cmd, cmd_dir, nohup, redirect)
        popens.append(run_cmd(ssh_cmd, popen))
