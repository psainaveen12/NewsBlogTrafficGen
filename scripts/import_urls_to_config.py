#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from loadtest.config_builder import build_default_config, build_targets, load_urls_from_files


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import URL list(s) into config/sites.yaml for randomized load tests."
    )
    parser.add_argument(
        "--input",
        required=True,
        nargs="+",
        help="One or more text files, each with one URL per line",
    )
    parser.add_argument(
        "--output",
        default="config/sites.yaml",
        help="Output YAML config path",
    )
    parser.add_argument(
        "--owner-authorization",
        choices=["true", "false"],
        default="false",
        help="Set true only when you own/have permission to test all targets",
    )
    return parser.parse_args()
def main() -> None:
    args = parse_args()
    input_paths = [Path(item).expanduser().resolve() for item in args.input]
    output_path = Path(args.output).resolve()
    owner_authorization = args.owner_authorization == "true"

    urls = load_urls_from_files(input_paths)
    targets = build_targets(urls)
    config = build_default_config(targets=targets, owner_authorization=owner_authorization)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    total_paths = sum(len(target["paths"]) for target in targets)
    total_query_paths = sum(len(target["query_paths"]) for target in targets)
    print(f"Imported URLs: {len(urls)}")
    print(f"Targets: {len(targets)}")
    print(f"Paths: {total_paths}")
    print(f"Query paths: {total_query_paths}")
    print(f"Wrote config: {output_path}")


if __name__ == "__main__":
    main()
