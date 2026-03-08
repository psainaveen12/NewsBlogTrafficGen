#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shlex
import subprocess
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run lightweight Colab smoke tests for all major runners.")
    parser.add_argument("--project-root", default="/content/NEWSBlogTrafficTest")
    parser.add_argument("--config", default="config/sites.yaml")
    parser.add_argument("--skip-threaded", action="store_true")
    parser.add_argument("--skip-locust", action="store_true")
    parser.add_argument("--skip-matrix", action="store_true")
    return parser.parse_args()


def run(cmd: list[str], cwd: Path) -> None:
    print(f"[smoke] $ {' '.join(shlex.quote(c) for c in cmd)}")
    subprocess.run(cmd, cwd=str(cwd), check=True)


def main() -> None:
    args = parse_args()
    root = Path(args.project_root).expanduser().resolve()

    run(["python3", "scripts/validate_config.py", "--config", args.config], cwd=root)

    if not args.skip_threaded:
        run(
            [
                "python3",
                "scripts/threaded_browser_journey.py",
                "--config",
                args.config,
                "--threads",
                "1",
                "--browser",
                "chromium",
                "--min-clicks",
                "1",
                "--max-clicks",
                "1",
                "--min-page-browse-seconds",
                "2",
                "--max-page-browse-seconds",
                "3",
                "--min-scroll-seconds",
                "1",
                "--max-scroll-seconds",
                "1.5",
                "--min-scroll-pause-seconds",
                "0.5",
                "--max-scroll-pause-seconds",
                "1",
                "--min-post-scroll-click-delay-seconds",
                "0.5",
                "--max-post-scroll-click-delay-seconds",
                "1",
                "--max-cycles-per-thread",
                "1",
            ],
            cwd=root,
        )

    if not args.skip_locust:
        run(
            [
                "python3",
                "scripts/run_locust.py",
                "--config",
                args.config,
                "--users",
                "15",
                "--spawn-rate",
                "3",
                "--run-time",
                "45s",
                "--results-dir",
                "results",
            ],
            cwd=root,
        )

    if not args.skip_matrix:
        run(
            [
                "python3",
                "scripts/browser_matrix.py",
                "--config",
                args.config,
                "--paths-per-target",
                "1",
                "--browsers",
                "chromium",
                "--output-json",
                "results/browser-matrix-colab-smoke.json",
            ],
            cwd=root,
        )

    print("[smoke] Completed.")


if __name__ == "__main__":
    main()
