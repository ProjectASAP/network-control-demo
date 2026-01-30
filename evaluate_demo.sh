#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT=10101

rm -f solver_experimental/query_rtt.csv \
      single_node_server/network-control-server/server_request_timing.csv \
      solver_experimental/e2e.csv \
      solver_experimental/query_compare.csv && sync

python3 "$ROOT_DIR/reset_es_index.py"

find_pids() {
  if command -v lsof >/dev/null 2>&1; then
    lsof -tiTCP:${PORT} -sTCP:LISTEN || true
  elif command -v fuser >/dev/null 2>&1; then
    fuser -n tcp ${PORT} 2>/dev/null || true
  elif command -v ss >/dev/null 2>&1; then
    ss -ltnp "sport = :${PORT}" 2>/dev/null | awk -F'pid=|,' 'NR>1 {print $2}' || true
  else
    return 0
  fi
}

stop_server() {
  local pids
  pids="$(find_pids)"
  if [[ -n "${pids}" ]]; then
    echo "Stopping server on port ${PORT} (pids: ${pids})"
    kill ${pids} || true
    for _ in {1..20}; do
      if [[ -z "$(find_pids)" ]]; then
        return 0
      fi
      sleep 0.5
    done
    echo "Server still running, sending SIGKILL"
    kill -9 ${pids} || true
  fi
}

wait_for_port() {
  python3 - "$PORT" "$1" <<'PY'
import os
import socket
import sys
import time

port = int(sys.argv[1])
pid = int(sys.argv[2])
deadline = time.time() + 300

while time.time() < deadline:
    s = socket.socket()
    try:
        s.settimeout(0.5)
        s.connect(("127.0.0.1", port))
        sys.exit(0)
    except Exception:
        pass
    finally:
        s.close()

    try:
        os.kill(pid, 0)
    except OSError:
        print("Server process exited before listening on the port.", file=sys.stderr)
        sys.exit(1)
    time.sleep(0.5)

print("Timed out waiting for server to start.", file=sys.stderr)
sys.exit(1)
PY
}

stop_server

echo "Starting server on port ${PORT}"
(
  cd "$ROOT_DIR/single_node_server/network-control-server"
  cargo run -- --timing > "$ROOT_DIR/server_10101.log" 2>&1 &
  echo $! > "$ROOT_DIR/server_10101.pid"
)
SERVER_PID="$(cat "$ROOT_DIR/server_10101.pid")"
wait_for_port "$SERVER_PID"

cd solver_experimental
bash run_main.sh
