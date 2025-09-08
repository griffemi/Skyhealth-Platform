from __future__ import annotations

import sys
import time
import requests
from google.cloud import compute_v1

from skyfeed.config import settings


def _metadata_token() -> str:
    url = (
        "http://metadata.google.internal/computeMetadata/v1/"
        "instance/service-accounts/default/token"
    )
    resp = requests.get(url, headers={"Metadata-Flavor": "Google"}, timeout=10)
    resp.raise_for_status()
    return resp.json()["access_token"]


def _resize_mig(size: int) -> None:
    project = settings.worker_project
    zone = settings.worker_zone
    mig = settings.worker_mig_name
    if not (project and zone and mig):
        return
    url = (
        "https://compute.googleapis.com/compute/v1/projects/"
        f"{project}/zones/{zone}/instanceGroupManagers/{mig}/resize"
    )
    token = _metadata_token()
    requests.post(url, params={"size": size}, headers={"Authorization": f"Bearer {token}"}, timeout=60).raise_for_status()


def _registered_workers() -> int:
    ui = settings.spark_master_ui_url
    try:
        resp = requests.get(f"{ui}/json", timeout=10)
        resp.raise_for_status()
        return len(resp.json().get("workers", []))
    except Exception:
        return 0


def _wait_for_workers(target: int, timeout_sec: int, poll_sec: int) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if _registered_workers() >= target:
            return
        time.sleep(poll_sec)
    raise RuntimeError("Worker nodes failed to register in time")


def _build_spark_props() -> dict:
    props = {
        "spark.master": settings.spark_master_url,
        "spark.app.name": "skyfeed-job",
        "spark.executor.cores": settings.spark_executor_cores,
        "spark.executor.memory": settings.spark_executor_memory,
        "spark.task.maxFailures": settings.spark_task_max_failures,
    }
    if settings.spark_dra_enable == 1:
        props.update(
            {
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.shuffleTracking.enabled": "true"
                if settings.spark_shuffle_tracking_enable == 1
                else "false",
                "spark.dynamicAllocation.minExecutors": str(settings.spark_exec_min),
                "spark.dynamicAllocation.initialExecutors": str(settings.spark_exec_init),
                "spark.dynamicAllocation.maxExecutors": str(settings.spark_exec_max),
            }
        )
    return props


def _submit(app: str, args: list[str]) -> str:
    payload = {
        "action": "CreateSubmissionRequest",
        "appResource": app,
        "clientSparkVersion": "3.5.0",
        "mainClass": "org.apache.spark.deploy.SparkSubmit",
        "appArgs": args,
        "sparkProperties": _build_spark_props(),
    }
    resp = requests.post(
        f"{settings.spark_master_rest_url}/v1/submissions/create", json=payload, timeout=60
    )
    resp.raise_for_status()
    return resp.json().get("submissionId", "")


def _monitor(submission_id: str) -> bool:
    interval = settings.poll_interval_sec
    timeout = settings.job_max_duration_sec
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = requests.get(
            f"{settings.spark_master_rest_url}/v1/submissions/status/{submission_id}", timeout=30
        )
        resp.raise_for_status()
        state = resp.json().get("driverState")
        if state in {"FINISHED", "ERROR", "FAILED", "KILLED"}:
            return state == "FINISHED"
        time.sleep(interval)
    return False


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else settings.mode
    args = sys.argv[2:] if len(sys.argv) > 1 else []
    apps = {
        "bronze-backfill": "local:///app/spark/jobs/bronze_openmeteo.py",
        "bronze-incremental": "local:///app/spark/jobs/bronze_openmeteo.py",
        "silver": "local:///app/spark/jobs/silver_climate_daily_features.py",
    }
    app = apps.get(mode, apps["silver"])
    if mode == "bronze-backfill":
        args = ["--mode", "backfill"] + args
    elif mode == "bronze-incremental":
        args = ["--mode", "incremental"] + args

    prescale = settings.worker_prescale
    postscale = settings.worker_postscale
    ready = settings.worker_ready_target or prescale
    timeout = settings.worker_register_timeout_sec
    poll = settings.worker_register_poll_sec
    dry_run = settings.dry_run == 1

    master_project = settings.master_project
    master_zone = settings.master_zone
    master_name = settings.master_instance_name
    comp_client = compute_v1.InstancesClient() if master_project else None

    def master_start():
        if not comp_client:
            return
        op = comp_client.start(project=master_project, zone=master_zone, instance=master_name)
        op.result(timeout=settings.master_start_timeout_sec)

    def master_stop():
        if not comp_client:
            return
        keep = settings.keep_master_after
        if keep:
            time.sleep(keep * 60)
        op = comp_client.stop(project=master_project, zone=master_zone, instance=master_name)
        op.result(timeout=settings.master_stop_timeout_sec)

    if comp_client:
        master_start()

    if prescale and not dry_run:
        _resize_mig(prescale)
        _wait_for_workers(ready, timeout, poll)

    if dry_run:
        if prescale and postscale != prescale:
            _resize_mig(postscale)
        return

    submission = _submit(app, args)
    ok = _monitor(submission)

    if postscale or prescale:
        _resize_mig(postscale)

    if comp_client:
        master_stop()

    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
