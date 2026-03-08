#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import ssl
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from loadtest.config import TestConfig, load_test_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run authorized multi-target load test with Locust.")
    parser.add_argument("--config", default="config/sites.yaml", help="YAML config file path")
    parser.add_argument("--users", type=int, default=50, help="Concurrent users")
    parser.add_argument("--spawn-rate", type=float, default=5.0, help="Users spawned per second")
    parser.add_argument("--run-time", default="10m", help="Test duration, ex: 30s, 5m, 1h")
    parser.add_argument("--results-dir", default="results", help="Directory for CSV and HTML output")
    parser.add_argument("--web-ui", action="store_true", help="Run with Locust web UI (no headless mode)")

    parser.add_argument(
        "--request-name-mode",
        choices=["label", "path"],
        default="label",
        help="'label' keeps low cardinality metrics; 'path' shows per-path metrics.",
    )
    parser.add_argument("--primary-task-weight", type=int, default=75)
    parser.add_argument("--query-task-weight", type=int, default=20)
    parser.add_argument("--health-task-weight", type=int, default=5)

    parser.add_argument("--disable-healthcheck", action="store_true", help="Disable health endpoint requests")
    parser.add_argument(
        "--auto-disable-healthcheck",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Disable health endpoint requests automatically when most health checks fail preflight.",
    )
    parser.add_argument(
        "--adaptive-backoff",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable adaptive per-user backoff when 429/503 responses are observed.",
    )
    parser.add_argument("--backoff-min-seconds", type=float, default=0.75)
    parser.add_argument("--backoff-max-seconds", type=float, default=6.0)
    parser.add_argument("--max-throttle-backoff-seconds", type=float, default=20.0)
    parser.add_argument("--preflight-timeout-seconds", type=float, default=12.0)

    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.users < 1:
        raise ValueError("--users must be >= 1")
    if args.spawn_rate <= 0:
        raise ValueError("--spawn-rate must be > 0")
    if args.primary_task_weight < 1:
        raise ValueError("--primary-task-weight must be >= 1")
    if args.query_task_weight < 0:
        raise ValueError("--query-task-weight must be >= 0")
    if args.health_task_weight < 0:
        raise ValueError("--health-task-weight must be >= 0")
    if args.backoff_min_seconds < 0:
        raise ValueError("--backoff-min-seconds must be >= 0")
    if args.backoff_max_seconds < args.backoff_min_seconds:
        raise ValueError("--backoff-max-seconds must be >= --backoff-min-seconds")
    if args.max_throttle_backoff_seconds < args.backoff_max_seconds:
        raise ValueError("--max-throttle-backoff-seconds must be >= --backoff-max-seconds")
    if args.preflight_timeout_seconds <= 0:
        raise ValueError("--preflight-timeout-seconds must be > 0")


def _http_status(url: str, timeout_seconds: float, ssl_verify: bool) -> int | None:
    context = None
    if url.startswith("https://") and not ssl_verify:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

    request = urllib.request.Request(
        url=url,
        method="GET",
        headers={"User-Agent": "LoadOps-Preflight/1.0", "Accept": "text/html,*/*;q=0.8"},
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds, context=context) as response:
            return int(getattr(response, "status", 200))
    except urllib.error.HTTPError as exc:
        return int(exc.code)
    except Exception:
        return None


def preflight_healthchecks(cfg: TestConfig, timeout_seconds: float) -> dict[str, object]:
    failing = 0
    checked = 0
    warnings: list[str] = []

    for target in cfg.targets:
        health_url = urljoin(f"{target.base_url}/", target.healthcheck_path.lstrip("/"))
        status = _http_status(
            url=health_url,
            timeout_seconds=timeout_seconds,
            ssl_verify=cfg.load_profile.ssl_verify,
        )
        checked += 1
        if status is None or status >= 400:
            failing += 1
            warnings.append(f"{target.name} -> {health_url} status={status}")

    return {"checked": checked, "failing": failing, "warnings": warnings}


def main() -> None:
    args = parse_args()
    validate_args(args)

    cfg = load_test_config(args.config)

    disable_healthcheck = bool(args.disable_healthcheck)
    if not disable_healthcheck and args.auto_disable_healthcheck:
        preflight = preflight_healthchecks(cfg, timeout_seconds=float(args.preflight_timeout_seconds))
        checked = int(preflight["checked"])
        failing = int(preflight["failing"])
        warnings = list(preflight["warnings"])
        if checked > 0:
            failure_ratio = failing / checked
            if failure_ratio >= 0.6:
                disable_healthcheck = True
                print(
                    "[run_locust] Auto-disabled healthcheck task: "
                    f"{failing}/{checked} health endpoints returned 4xx/5xx or were unreachable."
                )
            elif failing > 0:
                print(f"[run_locust] Healthcheck preflight warnings: {failing}/{checked} failing target(s).")
        for warning in warnings[:8]:
            print(f"[run_locust] preflight: {warning}")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    results_prefix = results_dir / f"loadtest-{timestamp}"

    env = os.environ.copy()
    env["LOAD_TEST_CONFIG"] = args.config
    env["LOADTEST_REQUEST_NAME_MODE"] = args.request_name_mode
    env["LOADTEST_PRIMARY_TASK_WEIGHT"] = str(int(args.primary_task_weight))
    env["LOADTEST_QUERY_TASK_WEIGHT"] = str(int(args.query_task_weight))
    env["LOADTEST_HEALTH_TASK_WEIGHT"] = str(int(args.health_task_weight))
    env["LOADTEST_DISABLE_HEALTHCHECK"] = "1" if disable_healthcheck else "0"
    env["LOADTEST_ENABLE_ADAPTIVE_BACKOFF"] = "1" if args.adaptive_backoff else "0"
    env["LOADTEST_BACKOFF_MIN_SECONDS"] = str(float(args.backoff_min_seconds))
    env["LOADTEST_BACKOFF_MAX_SECONDS"] = str(float(args.backoff_max_seconds))
    env["LOADTEST_MAX_THROTTLE_BACKOFF_SECONDS"] = str(float(args.max_throttle_backoff_seconds))

    if args.users >= 200 and args.spawn_rate >= 10:
        print(
            "[run_locust] Warning: very aggressive profile detected "
            f"(users={args.users}, spawn_rate={args.spawn_rate}). "
            "Expect 429/503 unless the target is provisioned for this level."
        )

    cmd = [
        sys.executable,
        "-m",
        "locust",
        "-f",
        "locustfile.py",
        "--users",
        str(args.users),
        "--spawn-rate",
        str(args.spawn_rate),
        "--run-time",
        args.run_time,
        "--csv",
        str(results_prefix),
        "--html",
        f"{results_prefix}.html",
    ]

    if not args.web_ui:
        cmd.append("--headless")

    result = subprocess.run(cmd, check=False, env=env, cwd=str(PROJECT_ROOT))
    if result.returncode not in {0, 1}:
        raise SystemExit(result.returncode)
    if result.returncode == 1:
        print("[run_locust] Locust exited with code 1 because failures were recorded. Artifacts were generated successfully.")


if __name__ == "__main__":
    main()
