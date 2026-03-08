from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml


@dataclass(frozen=True)
class WaitTimeConfig:
    min_seconds: float = 1.0
    max_seconds: float = 3.0


@dataclass(frozen=True)
class LoadProfileConfig:
    wait_time: WaitTimeConfig = field(default_factory=WaitTimeConfig)
    request_timeout_seconds: float = 20.0
    switch_target_probability: float = 0.12
    ssl_verify: bool = True


@dataclass(frozen=True)
class TrafficProfile:
    name: str
    weight: int
    user_agent: str


@dataclass(frozen=True)
class HeaderPool:
    accept_language: list[str] = field(default_factory=lambda: ["en-US,en;q=0.9"])
    cache_control: list[str] = field(default_factory=lambda: ["no-cache"])


@dataclass(frozen=True)
class TargetConfig:
    name: str
    base_url: str
    weight: int
    paths: list[str]
    query_paths: list[str] = field(default_factory=list)
    healthcheck_path: str = "/robots.txt"


@dataclass(frozen=True)
class TestConfig:
    owner_authorization: bool
    load_profile: LoadProfileConfig
    headers_pool: HeaderPool
    traffic_profiles: list[TrafficProfile]
    targets: list[TargetConfig]


class ConfigError(ValueError):
    pass


def _ensure_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConfigError(f"Invalid URL: {url}")
    return url.rstrip("/")


def _ensure_non_empty_paths(paths: list[str], target_name: str) -> list[str]:
    if not paths:
        raise ConfigError(f"Target '{target_name}' must define at least one path")
    normalized = []
    for path in paths:
        if not path.startswith("/"):
            path = f"/{path}"
        normalized.append(path)
    return normalized


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ConfigError("Top-level config must be a mapping")
    return data


def load_test_config(config_path: str | Path) -> TestConfig:
    path = Path(config_path)
    data = _read_yaml(path)

    owner_authorization = bool(data.get("owner_authorization", False))

    load_data = data.get("load_profile", {})
    wait_data = load_data.get("wait_time_seconds", {})
    wait_time = WaitTimeConfig(
        min_seconds=float(wait_data.get("min", 1.0)),
        max_seconds=float(wait_data.get("max", 3.0)),
    )
    if wait_time.min_seconds < 0 or wait_time.max_seconds < wait_time.min_seconds:
        raise ConfigError("Invalid wait_time_seconds bounds")

    load_profile = LoadProfileConfig(
        wait_time=wait_time,
        request_timeout_seconds=float(load_data.get("request_timeout_seconds", 20.0)),
        switch_target_probability=float(load_data.get("switch_target_probability", 0.12)),
        ssl_verify=bool(load_data.get("ssl_verify", True)),
    )
    if not 0 <= load_profile.switch_target_probability <= 1:
        raise ConfigError("switch_target_probability must be between 0 and 1")

    headers_data = data.get("headers_pool", {})
    headers_pool = HeaderPool(
        accept_language=list(headers_data.get("accept_language", ["en-US,en;q=0.9"])),
        cache_control=list(headers_data.get("cache_control", ["no-cache"])),
    )

    traffic_profiles_data = data.get("traffic_profiles", [])
    traffic_profiles: list[TrafficProfile] = []
    for entry in traffic_profiles_data:
        traffic_profiles.append(
            TrafficProfile(
                name=str(entry["name"]),
                weight=int(entry.get("weight", 1)),
                user_agent=str(entry["user_agent"]),
            )
        )
    if not traffic_profiles:
        raise ConfigError("At least one traffic profile is required")
    if any(profile.weight <= 0 for profile in traffic_profiles):
        raise ConfigError("Traffic profile weights must be > 0")

    targets_data = data.get("targets", [])
    targets: list[TargetConfig] = []
    for entry in targets_data:
        name = str(entry["name"])
        base_url = _ensure_url(str(entry["base_url"]))
        paths = _ensure_non_empty_paths(list(entry.get("paths", [])), name)
        query_paths = _ensure_non_empty_paths(list(entry.get("query_paths", [])), name) if entry.get("query_paths") else []
        healthcheck_path = str(entry.get("healthcheck_path", "/robots.txt"))
        if not healthcheck_path.startswith("/"):
            healthcheck_path = f"/{healthcheck_path}"

        targets.append(
            TargetConfig(
                name=name,
                base_url=base_url,
                weight=int(entry.get("weight", 1)),
                paths=paths,
                query_paths=query_paths,
                healthcheck_path=healthcheck_path,
            )
        )
    if not targets:
        raise ConfigError("At least one target is required")
    if any(target.weight <= 0 for target in targets):
        raise ConfigError("Target weights must be > 0")

    return TestConfig(
        owner_authorization=owner_authorization,
        load_profile=load_profile,
        headers_pool=headers_pool,
        traffic_profiles=traffic_profiles,
        targets=targets,
    )

