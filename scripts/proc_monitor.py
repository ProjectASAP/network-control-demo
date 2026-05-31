#!/usr/bin/env python3
"""OS-level, symmetric resource measurement for the resource benchmark.

Design goals (see plan polished-questing-quail.md):
  - Symmetric: the *same* external tool measures both the sketch server and ES.
    We never use ES's internal instrumentation (it would distort latency).
  - Near-zero overhead: CPU is read only at window boundaries from
    /proc/<pid>/stat; RSS is sampled by a background thread from
    /proc/<pid>/status. Neither touches the measured process, so latency,
    CPU, and RSS can all be captured in the SAME run.
  - No third-party deps, no privileges required (taskset on same-uid PIDs).

This module is import-safe and also runnable as `python3 proc_monitor.py
--selftest` to print the resolved PIDs and current counters.
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

# Clock ticks per second (jiffies). Typically 100 -> 10ms CPU resolution.
HZ = os.sysconf("SC_CLK_TCK")
PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")


# ---------------------------------------------------------------------------
# PID resolution
# ---------------------------------------------------------------------------

def _iter_proc_pids() -> List[int]:
    pids: List[int] = []
    for entry in os.listdir("/proc"):
        if entry.isdigit():
            pids.append(int(entry))
    return pids


def _read_cmdline(pid: int) -> str:
    """Return the full cmdline (NUL-separated args joined by spaces) or ''."""
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as fh:
            raw = fh.read()
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return ""
    return raw.replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()


def resolve_server_pid(needle: str = "network-control-server") -> Optional[int]:
    """Find the sketch server PID.

    The server is launched via `cargo run --release`, so the cargo PID is the
    parent and the real binary (`network-control-server`) is a child. We match
    the binary in the cmdline and prefer the process whose executable basename
    is exactly the binary (not the cargo wrapper).
    """
    candidates: List[int] = []
    for pid in _iter_proc_pids():
        cmd = _read_cmdline(pid)
        if not cmd:
            continue
        if needle in cmd and "cargo" not in cmd.split()[0]:
            candidates.append(pid)
    if not candidates:
        # Fallback: any process whose cmdline mentions the binary.
        for pid in _iter_proc_pids():
            if needle in _read_cmdline(pid):
                candidates.append(pid)
    if not candidates:
        return None
    # Prefer the highest PID (most recently started server).
    return max(candidates)


def resolve_es_pid(needle: str = "org.elasticsearch.bootstrap.Elasticsearch") -> Optional[int]:
    """Find the real Elasticsearch JVM PID (the bootstrap process, not the CLI launcher)."""
    for pid in _iter_proc_pids():
        if needle in _read_cmdline(pid):
            return pid
    return None


def proc_owner_uid(pid: int) -> Optional[int]:
    try:
        return os.stat(f"/proc/{pid}").st_uid
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None


# ---------------------------------------------------------------------------
# /proc counters
# ---------------------------------------------------------------------------

@dataclass
class CpuSnapshot:
    utime_ticks: int
    stime_ticks: int
    wall_s: float

    @property
    def total_ticks(self) -> int:
        return self.utime_ticks + self.stime_ticks


def read_cpu_ticks(pid: int) -> CpuSnapshot:
    """Read utime+stime (process-wide, includes all threads) from /proc/<pid>/stat.

    Field 2 (comm) may contain spaces and parentheses, so we parse everything
    after the final ')'. In that suffix, field 3 (state) is index 0, hence
    utime (field 14) is index 11 and stime (field 15) is index 12.
    """
    wall = time.monotonic()
    with open(f"/proc/{pid}/stat", "r", encoding="utf-8") as fh:
        data = fh.read()
    rparen = data.rfind(")")
    fields = data[rparen + 2:].split()
    utime = int(fields[11])
    stime = int(fields[12])
    return CpuSnapshot(utime_ticks=utime, stime_ticks=stime, wall_s=wall)


@dataclass
class RssSample:
    t: float          # time.monotonic()
    vmrss_kb: int
    vmhwm_kb: int


def read_rss(pid: int) -> RssSample:
    """Read VmRSS (current) and VmHWM (peak) in kB from /proc/<pid>/status."""
    vmrss = 0
    vmhwm = 0
    with open(f"/proc/{pid}/status", "r", encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("VmRSS:"):
                vmrss = int(line.split()[1])
            elif line.startswith("VmHWM:"):
                vmhwm = int(line.split()[1])
            if vmrss and vmhwm:
                break
    return RssSample(t=time.monotonic(), vmrss_kb=vmrss, vmhwm_kb=vmhwm)


# ---------------------------------------------------------------------------
# Background RSS sampler
# ---------------------------------------------------------------------------

@dataclass
class ResourceSampler:
    """Background-thread RSS poller for one PID.

    CPU is intentionally NOT polled here; callers read CPU at window
    boundaries via read_cpu_ticks for exact accounting. The sampler only
    records the RSS time series, which is cheap and external to the target.
    """

    pid: int
    poll_interval_s: float = 0.05
    samples: List[RssSample] = field(default_factory=list)
    _thread: Optional[threading.Thread] = None
    _stop: threading.Event = field(default_factory=threading.Event)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.samples.append(read_rss(self.pid))
            except (FileNotFoundError, ProcessLookupError):
                break
            self._stop.wait(self.poll_interval_s)

    def start(self) -> None:
        self.samples = []
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    # --- aggregates (MB) ---
    def _vmrss_mb(self) -> List[float]:
        return [s.vmrss_kb / 1024.0 for s in self.samples]

    def mean_rss_mb(self) -> float:
        vals = self._vmrss_mb()
        return sum(vals) / len(vals) if vals else float("nan")

    def max_rss_mb(self) -> float:
        vals = self._vmrss_mb()
        return max(vals) if vals else float("nan")

    def max_vmhwm_mb(self) -> float:
        if not self.samples:
            return float("nan")
        return max(s.vmhwm_kb for s in self.samples) / 1024.0

    def raw_series(self) -> List[Dict[str, float]]:
        return [
            {"t": s.t, "vmrss_kb": s.vmrss_kb, "vmhwm_kb": s.vmhwm_kb}
            for s in self.samples
        ]


# ---------------------------------------------------------------------------
# CPU pinning
# ---------------------------------------------------------------------------

def pin_cpu(pid: int, cores: str) -> bool:
    """Pin an existing process to `cores` (e.g. "0-19" or "0,1,2") via taskset.

    Returns True on success. ES and the benchmark run as the same uid, so this
    needs no root. Raises nothing; logs a warning and returns False on failure.
    """
    if not cores:
        return False
    try:
        res = subprocess.run(
            ["taskset", "-cp", cores, str(pid)],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        print("[proc_monitor] taskset not found; skipping CPU pinning")
        return False
    if res.returncode != 0:
        print(f"[proc_monitor] taskset failed for pid={pid} cores={cores}: {res.stderr.strip()}")
        return False
    return True


def current_affinity(pid: int) -> Optional[List[int]]:
    try:
        return sorted(os.sched_getaffinity(pid))
    except (OSError, AttributeError):
        return None


# ---------------------------------------------------------------------------
# Environment fingerprint
# ---------------------------------------------------------------------------

def env_fingerprint(server_pid: Optional[int], es_pid: Optional[int]) -> Dict[str, object]:
    info: Dict[str, object] = {
        "hz": HZ,
        "page_size": PAGE_SIZE,
        "nproc": os.cpu_count(),
        "self_pid": os.getpid(),
        "self_affinity": current_affinity(os.getpid()),
    }
    if server_pid is not None:
        info["server_pid"] = server_pid
        info["server_uid"] = proc_owner_uid(server_pid)
        info["server_affinity"] = current_affinity(server_pid)
        info["server_cmdline"] = _read_cmdline(server_pid)
    if es_pid is not None:
        info["es_pid"] = es_pid
        info["es_uid"] = proc_owner_uid(es_pid)
        info["es_affinity"] = current_affinity(es_pid)
        info["es_cmdline"] = _read_cmdline(es_pid)
    return info


def _selftest() -> None:
    print(f"HZ={HZ} PAGE_SIZE={PAGE_SIZE} nproc={os.cpu_count()}")
    server_pid = resolve_server_pid()
    es_pid = resolve_es_pid()
    print(f"server_pid={server_pid} es_pid={es_pid}")
    for label, pid in [("server", server_pid), ("es", es_pid)]:
        if pid is None:
            print(f"  {label}: NOT FOUND")
            continue
        cpu = read_cpu_ticks(pid)
        rss = read_rss(pid)
        print(
            f"  {label}: pid={pid} uid={proc_owner_uid(pid)} "
            f"cpu_total_ticks={cpu.total_ticks} (u={cpu.utime_ticks} s={cpu.stime_ticks}) "
            f"VmRSS={rss.vmrss_kb/1024:.1f}MB VmHWM={rss.vmhwm_kb/1024:.1f}MB "
            f"affinity={current_affinity(pid)}"
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="OS-level resource monitor helpers")
    parser.add_argument("--selftest", action="store_true", help="Print resolved PIDs and counters")
    ns = parser.parse_args()
    if ns.selftest:
        _selftest()
    else:
        parser.print_help()
