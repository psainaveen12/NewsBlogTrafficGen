#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from loadtest.config import ConfigError, load_test_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate load test configuration file.")
    parser.add_argument("--config", default="config/sites.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        cfg = load_test_config(args.config)
    except ConfigError as exc:
        raise SystemExit(f"Invalid config: {exc}") from exc

    print("Configuration is valid.")
    print(f"Targets: {len(cfg.targets)}")
    print(f"Traffic profiles: {len(cfg.traffic_profiles)}")
    print(f"Authorization: {cfg.owner_authorization}")


if __name__ == "__main__":
    main()
