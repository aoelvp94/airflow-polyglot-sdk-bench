"""Tiny stdlib-only client for the Airflow 3 REST API of the local demo stack."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = os.environ.get("AIRFLOW_API", f"http://localhost:{os.environ.get('AIRFLOW_PORT', '18080')}/api/v2")
TERMINAL = {"success", "failed"}


def _call(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} {path} -> {e.code}: {e.read().decode()[:500]}") from e


def wait_for_dag(dag_id: str, timeout: float = 300) -> None:
    deadline = time.time() + timeout
    while True:
        try:
            _call("GET", f"/dags/{dag_id}")
            return
        except RuntimeError:
            if time.time() > deadline:
                raise
            time.sleep(3)


def unpause(dag_id: str) -> None:
    _call("PATCH", f"/dags/{dag_id}", {"is_paused": False})


def trigger(dag_id: str, conf: dict) -> str:
    run = _call("POST", f"/dags/{dag_id}/dagRuns", {"logical_date": None, "conf": conf})
    return run["dag_run_id"]


def wait_run(dag_id: str, run_id: str, timeout: float = 1800) -> str:
    deadline = time.time() + timeout
    q = urllib.parse.quote(run_id, safe="")
    while time.time() < deadline:
        state = _call("GET", f"/dags/{dag_id}/dagRuns/{q}")["state"]
        if state in TERMINAL:
            return state
        time.sleep(2)
    raise TimeoutError(f"{dag_id}/{run_id} did not finish in {timeout}s")


def task_instances(dag_id: str, run_id: str) -> dict[str, dict]:
    q = urllib.parse.quote(run_id, safe="")
    out = _call("GET", f"/dags/{dag_id}/dagRuns/{q}/taskInstances?limit=100")
    return {ti["task_id"]: ti for ti in out["task_instances"]}


def xcom(dag_id: str, run_id: str, task_id: str, key: str = "return_value"):
    q = urllib.parse.quote(run_id, safe="")
    t = urllib.parse.quote(task_id, safe="")
    entry = _call("GET", f"/dags/{dag_id}/dagRuns/{q}/taskInstances/{t}/xcomEntries/{key}?deserialize=true")
    value = entry.get("value")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            pass
    return value
