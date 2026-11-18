"""Process metrics read from /proc/self, the same way the Go/Java/TS handlers read them."""

from __future__ import annotations

import time

CLK_TCK = 100.0  # USER_HZ on Linux


def _stat_fields() -> list[str]:
    with open("/proc/self/stat") as f:
        s = f.read()
    return s[s.rindex(")") + 2 :].split()


def _proc_cpu() -> float:
    f = _stat_fields()
    return (float(f[11]) + float(f[12])) / CLK_TCK


def _process_age() -> float:
    with open("/proc/uptime") as f:
        up = float(f.read().split()[0])
    return up - float(_stat_fields()[19]) / CLK_TCK


def _vm_hwm_kb() -> float:
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmHWM:"):
                return float(line.split()[1])
    return 0.0


class Metrics:
    def __init__(self) -> None:
        self.wall_start = time.perf_counter()
        self.cpu_start = _proc_cpu()
        self.age = _process_age()

    def finish(self, rows: int, bytes_in: int) -> dict:
        elapsed = time.perf_counter() - self.wall_start
        cpu_end = _proc_cpu()
        out = {
            "handler_elapsed_s": round(elapsed, 3),
            "process_age_at_start_s": round(self.age, 3),
            "cpu_handler_s": round(cpu_end - self.cpu_start, 3),
            "cpu_process_total_s": round(cpu_end, 3),
            "peak_rss_mb": round(_vm_hwm_kb() / 1024, 3),
            "bytes_in": bytes_in,
        }
        if elapsed > 0 and rows > 0:
            out["rows_per_s"] = int(rows / elapsed)
        return out
