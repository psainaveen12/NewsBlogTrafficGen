from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

from loadtest.user_agents import top_25_traffic_profiles_dicts


DEFAULT_LOAD_PROFILE: dict = {
    "wait_time_seconds": {"min": 0.5, "max": 2.5},
    "request_timeout_seconds": 25,
    "switch_target_probability": 0.2,
    "ssl_verify": True,
}

DEFAULT_HEADERS_POOL: dict = {
    "accept_language": ["en-US,en;q=0.9", "en-GB,en;q=0.9", "en-IN,en;q=0.8"],
    "cache_control": ["no-cache", "max-age=0"],
}


def normalize_path(path: str) -> str:
    if not path:
        return "/"
    if not path.startswith("/"):
        return f"/{path}"
    return path


def safe_target_name(base_url: str) -> str:
    parsed = urlparse(base_url)
    host = parsed.netloc.replace(":", "_")
    slug = re.sub(r"[^a-zA-Z0-9_]+", "_", host).strip("_").lower()
    return f"target_{slug}"


def load_urls_from_files(paths: list[Path]) -> list[str]:
    urls: list[str] = []
    for path in paths:
        if not path.exists():
            raise FileNotFoundError(f"Input URL file not found: {path}")
        raw_lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        urls.extend(line.strip() for line in raw_lines if line.strip())
    if not urls:
        raise ValueError("Input URL files are empty")
    return urls


def build_targets(urls: list[str]) -> list[dict]:
    grouped: dict[str, dict[str, set[str]]] = defaultdict(
        lambda: {"paths": set(), "query_paths": set()}
    )
    counts: dict[str, int] = defaultdict(int)

    for url in urls:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            continue
        base_url = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
        path = normalize_path(parsed.path)
        if parsed.query:
            grouped[base_url]["query_paths"].add(f"{path}?{parsed.query}")
        else:
            grouped[base_url]["paths"].add(path)
        counts[base_url] += 1

    targets: list[dict] = []
    for base_url in sorted(grouped.keys()):
        entry = grouped[base_url]
        paths = sorted(entry["paths"])
        if "/" not in paths:
            paths.insert(0, "/")
        targets.append(
            {
                "name": safe_target_name(base_url),
                "base_url": base_url,
                "weight": max(1, counts[base_url]),
                "paths": paths,
                "query_paths": sorted(entry["query_paths"]),
                "healthcheck_path": "/",
            }
        )
    if not targets:
        raise ValueError("No valid URLs were parsed from input")
    return targets


def build_default_config(targets: list[dict], owner_authorization: bool) -> dict:
    return {
        "owner_authorization": owner_authorization,
        "load_profile": DEFAULT_LOAD_PROFILE,
        "headers_pool": DEFAULT_HEADERS_POOL,
        "traffic_profiles": top_25_traffic_profiles_dicts(),
        "targets": targets,
    }

