from __future__ import annotations

import os
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin

import gevent
from locust import HttpUser, between, task

from loadtest.config import ConfigError, TestConfig, load_test_config
from loadtest.randomization import weighted_choice


def _build_url(base_url: str, path: str) -> str:
    return urljoin(f"{base_url}/", path.lstrip("/"))


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, min_value: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(min_value, value)


def _env_float(name: str, default: float, min_value: float = 0.0) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return max(min_value, value)


def _load_config() -> TestConfig:
    config_path = os.getenv("LOAD_TEST_CONFIG", "config/sites.yaml")
    config = load_test_config(config_path)
    if not config.owner_authorization:
        raise ConfigError(
            "owner_authorization must be true in config/sites.yaml before running tests. "
            "Run tests only against systems you own or have explicit permission to test."
        )
    return config


CONFIG = _load_config()

REQUEST_NAME_MODE = os.getenv("LOADTEST_REQUEST_NAME_MODE", "label").strip().lower()
if REQUEST_NAME_MODE not in {"label", "path"}:
    REQUEST_NAME_MODE = "label"

PRIMARY_TASK_WEIGHT = _env_int("LOADTEST_PRIMARY_TASK_WEIGHT", 75, min_value=1)
QUERY_TASK_WEIGHT = _env_int("LOADTEST_QUERY_TASK_WEIGHT", 20, min_value=0)
HEALTH_TASK_WEIGHT = _env_int("LOADTEST_HEALTH_TASK_WEIGHT", 5, min_value=0)

DISABLE_HEALTHCHECK = _env_bool("LOADTEST_DISABLE_HEALTHCHECK", False)
ENABLE_ADAPTIVE_BACKOFF = _env_bool("LOADTEST_ENABLE_ADAPTIVE_BACKOFF", True)
BACKOFF_MIN_SECONDS = _env_float("LOADTEST_BACKOFF_MIN_SECONDS", 0.75, min_value=0.0)
BACKOFF_MAX_SECONDS = _env_float("LOADTEST_BACKOFF_MAX_SECONDS", 6.0, min_value=BACKOFF_MIN_SECONDS)
MAX_THROTTLE_BACKOFF_SECONDS = _env_float(
    "LOADTEST_MAX_THROTTLE_BACKOFF_SECONDS",
    20.0,
    min_value=BACKOFF_MAX_SECONDS,
)


class MultiSiteLoadUser(HttpUser):
    host = "https://localhost"
    wait_time = between(
        CONFIG.load_profile.wait_time.min_seconds,
        CONFIG.load_profile.wait_time.max_seconds,
    )

    def on_start(self) -> None:
        self.current_target = weighted_choice(CONFIG.targets, lambda x: x.weight)
        self.last_path = "/"
        self._throttle_streak = 0
        self._throttle_until = 0.0

    def _maybe_rotate_target(self) -> None:
        if random.random() <= CONFIG.load_profile.switch_target_probability:
            self.current_target = weighted_choice(CONFIG.targets, lambda x: x.weight)

    def _make_request_name(self, target_name: str, label: str, path: str) -> str:
        if REQUEST_NAME_MODE == "path":
            path_label = path if len(path) <= 140 else f"{path[:137]}..."
            return f"{target_name}::{label}:{path_label}"
        return f"{target_name}::{label}"

    def _parse_retry_after_seconds(self, response) -> float | None:
        retry_after = (response.headers or {}).get("Retry-After")
        if not retry_after:
            return None
        retry_after = retry_after.strip()

        try:
            return max(0.0, float(retry_after))
        except ValueError:
            pass

        try:
            parsed = parsedate_to_datetime(retry_after)
        except (TypeError, ValueError):
            return None

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        delay = (parsed - datetime.now(timezone.utc)).total_seconds()
        return max(0.0, delay)

    def _sleep_if_throttled(self) -> None:
        if not ENABLE_ADAPTIVE_BACKOFF:
            return
        remaining = self._throttle_until - time.monotonic()
        if remaining > 0:
            gevent.sleep(remaining)

    def _apply_throttle_backoff(self, response) -> None:
        if not ENABLE_ADAPTIVE_BACKOFF:
            return

        status = response.status_code
        if status not in {429, 503}:
            self._throttle_streak = 0
            self._throttle_until = 0.0
            return

        self._throttle_streak = min(self._throttle_streak + 1, 7)
        retry_after = self._parse_retry_after_seconds(response)
        base_delay = retry_after if retry_after is not None else random.uniform(BACKOFF_MIN_SECONDS, BACKOFF_MAX_SECONDS)
        delay = min(MAX_THROTTLE_BACKOFF_SECONDS, base_delay * (2 ** (self._throttle_streak - 1)))
        self._throttle_until = max(self._throttle_until, time.monotonic() + delay)

    def _request(self, path: str, label: str) -> None:
        self._sleep_if_throttled()
        self._maybe_rotate_target()

        traffic_profile = weighted_choice(CONFIG.traffic_profiles, lambda x: x.weight)
        target = self.current_target
        url = _build_url(target.base_url, path)
        referer = _build_url(target.base_url, self.last_path)
        headers = {
            "User-Agent": traffic_profile.user_agent,
            "Accept-Language": random.choice(CONFIG.headers_pool.accept_language),
            "Cache-Control": random.choice(CONFIG.headers_pool.cache_control),
            "Referer": referer,
        }
        request_name = self._make_request_name(target.name, label, path)

        with self.client.get(
            url,
            name=request_name,
            headers=headers,
            timeout=CONFIG.load_profile.request_timeout_seconds,
            verify=CONFIG.load_profile.ssl_verify,
            catch_response=True,
        ) as response:
            if response.status_code in {429, 503}:
                self._apply_throttle_backoff(response)
                response.failure(f"Rate-limited/overloaded: {response.status_code}")
            elif response.status_code >= 500:
                self._apply_throttle_backoff(response)
                response.failure(f"Server error: {response.status_code}")
            elif response.status_code >= 400:
                self._apply_throttle_backoff(response)
                response.failure(f"Client error: {response.status_code}")
            else:
                self._throttle_streak = 0
                self._throttle_until = 0.0
                response.success()

        self.last_path = path

    @task(PRIMARY_TASK_WEIGHT)
    def browse_primary_pages(self) -> None:
        path = random.choice(self.current_target.paths)
        self._request(path=path, label="primary")

    @task(QUERY_TASK_WEIGHT if QUERY_TASK_WEIGHT > 0 else 1)
    def browse_query_pages(self) -> None:
        if QUERY_TASK_WEIGHT <= 0:
            return
        if not self.current_target.query_paths:
            self.browse_primary_pages()
            return
        path = random.choice(self.current_target.query_paths)
        self._request(path=path, label="query")

    @task(HEALTH_TASK_WEIGHT if HEALTH_TASK_WEIGHT > 0 else 1)
    def hit_health_endpoints(self) -> None:
        if DISABLE_HEALTHCHECK or HEALTH_TASK_WEIGHT <= 0:
            return
        self._request(path=self.current_target.healthcheck_path, label="health")
