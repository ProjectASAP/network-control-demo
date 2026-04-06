#!/usr/bin/env bash
# server.sh — start/stop the network-control-server
#
# Usage:
#   ./server.sh start [-- <cargo run args...>]
#   ./server.sh stop
#   ./server.sh restart [-- <cargo run args...>]
#   ./server.sh status

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/.server.pid"
LOG_FILE="$SCRIPT_DIR/.server.log"

cmd="${1:-}"
shift || true

case "$cmd" in
  start)
    if [[ -f "$PID_FILE" ]]; then
      pid="$(cat "$PID_FILE")"
      if kill -0 "$pid" 2>/dev/null; then
        echo "server is already running (pid $pid)" >&2
        exit 1
      fi
      rm -f "$PID_FILE"
    fi

    echo "building server..."
    cargo build --manifest-path "$SCRIPT_DIR/Cargo.toml" 2>&1 | tail -5

    echo "starting server (log: $LOG_FILE)..."
    cargo run --manifest-path "$SCRIPT_DIR/Cargo.toml" -- "$@" \
      >> "$LOG_FILE" 2>&1 &
    pid=$!
    echo "$pid" > "$PID_FILE"

    # Wait briefly and confirm the process is still alive
    sleep 1
    if kill -0 "$pid" 2>/dev/null; then
      echo "server started (pid $pid)"
    else
      echo "server failed to start — check $LOG_FILE" >&2
      rm -f "$PID_FILE"
      exit 1
    fi
    ;;

  stop)
    if [[ ! -f "$PID_FILE" ]]; then
      echo "no PID file found; server may not be running" >&2
      exit 1
    fi

    pid="$(cat "$PID_FILE")"
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "process $pid is not running (stale PID file removed)"
      rm -f "$PID_FILE"
      exit 0
    fi

    echo "sending SIGTERM to pid $pid..."
    kill -TERM "$pid"

    # Wait up to 10 s for graceful shutdown
    for i in $(seq 1 10); do
      if ! kill -0 "$pid" 2>/dev/null; then
        echo "server stopped"
        rm -f "$PID_FILE"
        exit 0
      fi
      sleep 1
    done

    echo "server did not stop in 10 s — sending SIGKILL..."
    kill -KILL "$pid" 2>/dev/null || true
    rm -f "$PID_FILE"
    echo "server killed"
    ;;

  restart)
    "$0" stop || true
    sleep 1
    "$0" start "$@"
    ;;

  status)
    if [[ ! -f "$PID_FILE" ]]; then
      echo "stopped (no PID file)"
      exit 1
    fi
    pid="$(cat "$PID_FILE")"
    if kill -0 "$pid" 2>/dev/null; then
      echo "running (pid $pid)"
    else
      echo "stopped (stale PID file)"
      rm -f "$PID_FILE"
      exit 1
    fi
    ;;

  *)
    echo "usage: $0 {start|stop|restart|status} [-- <args>]" >&2
    exit 1
    ;;
esac
