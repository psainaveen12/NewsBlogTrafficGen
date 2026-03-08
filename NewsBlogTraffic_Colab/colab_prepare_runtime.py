#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare NEWSBlogTrafficTest runtime files for Google Colab.")
    parser.add_argument("--project-root", default="/content/NEWSBlogTrafficTest")
    parser.add_argument("--config", default="config/sites.yaml")
    parser.add_argument(
        "--owner-authorization",
        choices=["keep", "true", "false"],
        default="false",
        help="Set owner_authorization in config/sites.yaml.",
    )
    return parser.parse_args()


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    return loaded if isinstance(loaded, dict) else {}


def _save_yaml(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _save_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _normalize_path(value: Any, fallback: str) -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    if text.startswith("/Users/") or text.startswith("C:\\"):
        return fallback
    return text


def prepare(project_root: Path, config_rel: str, owner_auth_mode: str) -> None:
    config_path = (project_root / config_rel).resolve()
    example_path = (project_root / "config/sites.example.yaml").resolve()

    if not config_path.exists() and example_path.exists():
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(example_path.read_text(encoding="utf-8"), encoding="utf-8")
        print(f"[prepare] Created config from example: {config_path}")

    cfg = _load_yaml(config_path)
    if not cfg:
        cfg = {
            "owner_authorization": False,
            "targets": [],
            "traffic_profiles": [],
        }

    if owner_auth_mode != "keep":
        cfg["owner_authorization"] = owner_auth_mode == "true"

    _save_yaml(config_path, cfg)

    threaded_ui_path = (project_root / "config/threaded_runner_ui.json").resolve()
    threaded_ui = _load_json(threaded_ui_path)
    threaded_ui_defaults = {
        "thr_threads": 5,
        "thr_browser": "chromium",
        "thr_min_clicks": 3,
        "thr_max_clicks": 4,
        "thr_cycle_limit": 0,
        "thr_nav_timeout": 45.0,
        "thr_headed": False,
        "thr_metrics_output": "results/threaded_journey_metrics.json",
        "thr_metrics_flush": 2.0,
        "thr_min_page_browse": 20.0,
        "thr_max_page_browse": 30.0,
        "thr_min_scroll": 6.0,
        "thr_max_scroll": 12.0,
        "thr_min_scroll_pause": 1.0,
        "thr_max_scroll_pause": 3.0,
        "thr_min_post_scroll_delay": 2.0,
        "thr_max_post_scroll_delay": 5.0,
        "thr_min_site_browse": 0.0,
        "thr_max_site_browse": 0.0,
        "thr_scroll_step": 700,
        "thr_disable_scroll": False,
    }
    for key, default in threaded_ui_defaults.items():
        threaded_ui.setdefault(key, default)
    threaded_ui["thr_metrics_output"] = _normalize_path(
        threaded_ui.get("thr_metrics_output"),
        "results/threaded_journey_metrics.json",
    )
    _save_json(threaded_ui_path, threaded_ui)

    comment_ui_path = (project_root / "config/comment_runner_ui.json").resolve()
    comment_ui = _load_json(comment_ui_path)
    comment_ui_defaults = {
        "com_threads": 5,
        "com_browser": "chromium",
        "com_cycle_limit": 0,
        "com_nav_timeout": 45.0,
        "com_headed": False,
        "com_metrics_output": "results/comment_journey_metrics.json",
        "com_metrics_flush": 2.0,
        "com_min_wait_seconds": 5.0,
        "com_max_wait_seconds": 10.0,
        "com_comment_profile_mode": "anonymous",
        "com_comment_names_file": "config/1000_random_names.txt",
        "com_comments_file": "config/comments_10000.txt",
    }
    for key, default in comment_ui_defaults.items():
        comment_ui.setdefault(key, default)
    comment_ui["com_metrics_output"] = _normalize_path(
        comment_ui.get("com_metrics_output"),
        "results/comment_journey_metrics.json",
    )
    comment_ui["com_comment_names_file"] = _normalize_path(
        comment_ui.get("com_comment_names_file"),
        "config/1000_random_names.txt",
    )
    comment_ui["com_comments_file"] = _normalize_path(
        comment_ui.get("com_comments_file"),
        "config/comments_10000.txt",
    )
    _save_json(comment_ui_path, comment_ui)

    (project_root / "results").mkdir(parents=True, exist_ok=True)

    print("[prepare] Runtime files normalized for Colab")
    print(f"[prepare] Config: {config_path}")
    print(f"[prepare] owner_authorization: {cfg.get('owner_authorization')}")
    print(f"[prepare] Threaded UI: {threaded_ui_path}")
    print(f"[prepare] Comment UI: {comment_ui_path}")


def main() -> None:
    args = parse_args()
    project_root = Path(args.project_root).expanduser().resolve()
    prepare(project_root, args.config, args.owner_authorization)


if __name__ == "__main__":
    main()
