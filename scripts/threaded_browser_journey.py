#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import logging
import random
import re
import signal
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from loadtest.config import ConfigError, TestConfig, TrafficProfile, load_test_config
from loadtest.playwright_install import ensure_playwright_browsers
from loadtest.user_agents import TOP_25_USER_AGENTS


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def status_bucket(status_code: int | None) -> str:
    if status_code is None:
        return "none"
    if 200 <= status_code < 300:
        return "2xx"
    if 300 <= status_code < 400:
        return "3xx"
    if 400 <= status_code < 500:
        return "4xx"
    if 500 <= status_code < 600:
        return "5xx"
    return "other"


@dataclass
class RunnerArgs:
    config: str
    threads: int
    browser: str
    min_clicks: int
    max_clicks: int
    min_page_browse_seconds: float
    max_page_browse_seconds: float
    min_scroll_seconds: float
    max_scroll_seconds: float
    min_scroll_pause_seconds: float
    max_scroll_pause_seconds: float
    min_post_scroll_click_delay_seconds: float
    max_post_scroll_click_delay_seconds: float
    min_site_browse_seconds: float
    max_site_browse_seconds: float
    scroll_step_px: int
    disable_scroll: bool
    navigation_timeout_seconds: float
    max_cycles_per_thread: int
    headless: bool
    metrics_output: str
    metrics_flush_seconds: float
    enable_comments: bool
    comment_profile_mode: str
    comment_names_file: str
    comments_file: str
    comment_attempt_rate: float
    comment_simple_mode: bool
    comment_min_wait_seconds: float
    comment_max_wait_seconds: float


@dataclass
class EngagementStats:
    page_browse_target_seconds: float = 0.0
    page_browse_actual_seconds: float = 0.0
    scroll_target_seconds: float = 0.0
    scroll_actual_seconds: float = 0.0
    scroll_actions: int = 0
    scroll_pixels: int = 0
    scroll_pause_seconds: float = 0.0
    post_scroll_delay_seconds: float = 0.0
    remainder_wait_seconds: float = 0.0


@dataclass
class CommentEngine:
    enabled: bool = False
    profile_mode: str = "anonymous"
    comment_attempt_rate: float = 1.0
    names: list[str] | None = None
    comments: list[str] | None = None


COMMENT_TEXTAREA_SELECTORS = [
    "textarea#comment",
    "textarea[name='comment']",
    "textarea[name*=\"comment\" i]",
    "textarea[placeholder*=\"comment\" i]",
    "textarea[aria-label*=\"comment\" i]",
    "form textarea",
]
COMMENT_SUBMIT_SELECTORS = [
    "button:has-text('Publish')",
    "button:has-text('Post Comment')",
    "button:has-text('Post')",
    "button:has-text('Submit')",
    "input[type='submit'][value*='Publish' i]",
    "input[type='submit'][value*='Post' i]",
    "input[type='submit'][value*='Submit' i]",
    "form button[type='submit']",
    "form input[type='submit']",
]
COMMENT_NAME_SELECTORS = [
    "input#author",
    "input[name='author']",
    "input[name='name']",
    "input[name*=\"name\" i]",
    "input[placeholder*=\"name\" i]",
]
PROFILE_ANON_SELECTORS = [
    "text=Anonymous",
    "text='Anonymous'",
]
PROFILE_NAME_SELECTORS = [
    "text=Name/URL",
    "text=Name / URL",
    "text='Name/URL'",
]
CAPTCHA_SELECTORS = [
    "iframe[src*='recaptcha']",
    ".g-recaptcha",
    "div[id*='captcha' i]",
]
STOP_WORDS = {
    "about", "after", "again", "all", "also", "an", "and", "any", "are", "article", "because",
    "been", "before", "between", "blog", "both", "but", "can", "could", "does", "each", "for",
    "from", "have", "here", "into", "just", "more", "most", "much", "news", "over", "page",
    "post", "really", "site", "some", "such", "than", "that", "the", "their", "them", "there",
    "these", "they", "this", "those", "very", "want", "what", "when", "where", "which", "while",
    "with", "would", "your",
}


def resolve_data_file_path(path_text: str) -> Path:
    path = Path(path_text).expanduser()
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


def parse_rtf_lines(raw_text: str) -> list[str]:
    text = raw_text.replace("\\par", "\n")
    text = re.sub(r"\\'[0-9a-fA-F]{2}", " ", text)
    text = re.sub(r"\\[a-zA-Z]+-?\d* ?", " ", text)
    text = text.replace("{", " ").replace("}", " ")
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = re.sub(r"\s+", " ", raw_line).strip()
        line = re.sub(r"^\d+[).\-\s]+", "", line).strip()
        if line and re.search(r"[A-Za-z]", line):
            lines.append(line)
    return lines


def load_text_values_from_file(path_text: str, field_label: str) -> list[str]:
    file_path = resolve_data_file_path(path_text)
    if not file_path.exists() or not file_path.is_file():
        raise SystemExit(f"{field_label} file not found: {file_path}")

    suffix = file_path.suffix.lower()
    if suffix == ".json":
        loaded = json.loads(file_path.read_text(encoding="utf-8"))
        if not isinstance(loaded, list):
            raise SystemExit(f"{field_label} JSON must be a list of strings")
        values = [str(item).strip() for item in loaded if str(item).strip()]
    elif suffix == ".csv":
        values = []
        with file_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            for row in reader:
                if not row:
                    continue
                candidate = str(row[0]).strip()
                if candidate:
                    values.append(candidate)
    elif suffix == ".rtf":
        values = parse_rtf_lines(file_path.read_text(encoding="utf-8", errors="ignore"))
    else:
        values = [line.strip() for line in file_path.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]

    if not values:
        raise SystemExit(f"{field_label} file has no usable entries: {file_path}")
    return values


def tokenize_for_match(text: str) -> set[str]:
    tokens = {
        token
        for token in re.findall(r"[a-zA-Z]{4,}", (text or "").lower())
        if token not in STOP_WORDS
    }
    return tokens


def pick_contextual_comment(page, comments: list[str], rng: random.Random) -> str:
    if not comments:
        return "Great article."

    title = ""
    headings = ""
    body_excerpt = ""

    try:
        title = page.title() or ""
    except Exception:
        title = ""

    try:
        headings = page.eval_on_selector_all(
            "h1, h2, h3",
            "els => els.slice(0, 6).map(el => (el.innerText || '').trim()).join(' ')",
        ) or ""
    except Exception:
        headings = ""

    try:
        body_excerpt = page.evaluate(
            "() => ((document.querySelector('article') || document.body)?.innerText || '').slice(0, 3000)"
        ) or ""
    except Exception:
        body_excerpt = ""

    context_tokens = tokenize_for_match(f"{title} {headings} {body_excerpt}")
    if not context_tokens:
        return rng.choice(comments)

    sample_size = min(len(comments), 600)
    sampled = rng.sample(comments, sample_size) if len(comments) > sample_size else list(comments)

    best_comment = None
    best_score = 0
    for candidate in sampled:
        candidate_tokens = tokenize_for_match(candidate)
        if not candidate_tokens:
            continue
        score = len(context_tokens.intersection(candidate_tokens))
        if score > best_score:
            best_score = score
            best_comment = candidate

    if best_comment and best_score > 0:
        return best_comment
    return rng.choice(sampled if sampled else comments)


def pick_comment_profile(profile_mode: str, rng: random.Random) -> str:
    mode = (profile_mode or "anonymous").strip().lower()
    if mode == "name":
        return "name"
    if mode == "mixed":
        return rng.choice(["anonymous", "name"])
    return "anonymous"


def find_first_visible_locator(page, selectors: list[str], timeout_ms: int = 1000):
    scopes = []
    try:
        scopes = list(page.frames)
    except Exception:
        scopes = [page]

    for scope in scopes:
        for selector in selectors:
            try:
                locator = scope.locator(selector).first
                if locator.count() == 0:
                    continue
                if locator.is_visible(timeout=timeout_ms):
                    return locator
            except Exception:
                continue
    return None


def maybe_click_profile_option(page, selectors: list[str], timeout_ms: int = 900) -> bool:
    scopes = []
    try:
        scopes = list(page.frames)
    except Exception:
        scopes = [page]

    for scope in scopes:
        for selector in selectors:
            try:
                locator = scope.locator(selector).first
                if locator.count() == 0:
                    continue
                locator.click(timeout=timeout_ms)
                return True
            except Exception:
                continue
    return False


def detect_captcha(page) -> bool:
    scopes = []
    try:
        scopes = list(page.frames)
    except Exception:
        scopes = [page]

    for scope in scopes:
        for selector in CAPTCHA_SELECTORS:
            try:
                if scope.locator(selector).count() > 0:
                    return True
            except Exception:
                continue
    try:
        page_text = (page.inner_text("body") or "").lower()
        if "captcha" in page_text and "publish" in page_text:
            return True
    except Exception:
        pass
    return False


def post_comment_on_page(
    page,
    worker_id: int,
    args: RunnerArgs,
    comment_engine: CommentEngine,
    metrics: ThreadMetricsCollector,
    stop_event: threading.Event,
    rng: random.Random,
) -> None:
    if not comment_engine.enabled or stop_event.is_set():
        return

    if rng.random() > comment_engine.comment_attempt_rate:
        metrics.record_comment_event(worker_id=worker_id, skipped_by_rate=True)
        return

    profile = pick_comment_profile(comment_engine.profile_mode, rng)
    comment_attempted = True
    form_detected = False
    form_missing = False
    comment_text_filled = False
    comment_name_filled = False
    publish_attempted = False
    publish_success = False
    captcha_detected = False
    error_kind = None

    try:
        if detect_captcha(page):
            captcha_detected = True
            error_kind = "comment_captcha_detected"
            return

        comment_box = find_first_visible_locator(page, COMMENT_TEXTAREA_SELECTORS, timeout_ms=1200)
        if comment_box is None:
            form_missing = True
            error_kind = "comment_form_missing"
            return
        form_detected = True

        comments_pool = comment_engine.comments or []
        if not comments_pool:
            error_kind = "comment_pool_empty"
            return

        selected_comment = pick_contextual_comment(page, comments_pool, rng)
        comment_box.click(timeout=1200)
        comment_box.fill(selected_comment, timeout=2000)
        comment_text_filled = True

        if profile == "name":
            names_pool = comment_engine.names or []
            if not names_pool:
                error_kind = "comment_names_pool_empty"
                return
            maybe_click_profile_option(page, PROFILE_NAME_SELECTORS)
            name_box = find_first_visible_locator(page, COMMENT_NAME_SELECTORS, timeout_ms=1000)
            if name_box is None:
                error_kind = "comment_name_input_missing"
                return
            selected_name = rng.choice(names_pool)
            name_box.fill(selected_name, timeout=1500)
            comment_name_filled = True
        else:
            maybe_click_profile_option(page, PROFILE_ANON_SELECTORS)

        submit_button = find_first_visible_locator(page, COMMENT_SUBMIT_SELECTORS, timeout_ms=1200)
        if submit_button is None:
            error_kind = "comment_publish_button_missing"
            return

        publish_attempted = True
        submit_button.click(timeout=max(1500, int(args.navigation_timeout_seconds * 1000)))
        sleep_interruptible(rng.uniform(0.8, 1.6), stop_event)
        publish_success = True
    except Exception as exc:  # noqa: BLE001
        if publish_attempted:
            error_kind = "comment_publish_click_error"
        else:
            error_kind = "comment_post_exception"
        logging.debug("Comment posting issue on worker %d: %s", worker_id, exc)
    finally:
        metrics.record_comment_event(
            worker_id=worker_id,
            profile=profile,
            comment_attempted=comment_attempted,
            form_detected=form_detected,
            form_missing=form_missing,
            comment_text_filled=comment_text_filled,
            comment_name_filled=comment_name_filled,
            publish_attempted=publish_attempted,
            publish_success=publish_success,
            captcha_detected=captcha_detected,
            skipped_by_rate=False,
            error_kind=error_kind,
        )


def build_comment_engine(args: RunnerArgs) -> CommentEngine:
    if not args.enable_comments:
        return CommentEngine(enabled=False)

    comments = load_text_values_from_file(args.comments_file, "Comments")

    names: list[str] = []
    if args.comment_profile_mode in {"name", "mixed"}:
        names = load_text_values_from_file(args.comment_names_file, "Names")

    return CommentEngine(
        enabled=True,
        profile_mode=args.comment_profile_mode,
        comment_attempt_rate=args.comment_attempt_rate,
        names=names,
        comments=comments,
    )


class UrlAllocator:
    """Tries to keep each worker on a different start URL at the same time."""

    def __init__(self, urls: list[str]) -> None:
        if not urls:
            raise ValueError("No URLs available for allocation")
        self._urls = urls
        self._active: set[str] = set()
        self._lock = threading.Lock()

    def acquire(self, rng: random.Random) -> str:
        with self._lock:
            free = [url for url in self._urls if url not in self._active]
            pool = free if free else self._urls
            choice = rng.choice(pool)
            self._active.add(choice)
            return choice

    def release(self, url: str) -> None:
        with self._lock:
            self._active.discard(url)


class ThreadMetricsCollector:
    def __init__(
        self,
        args: RunnerArgs,
        config: TestConfig,
        start_url_count: int,
        stop_event: threading.Event,
    ) -> None:
        output_path = Path(args.metrics_output).expanduser()
        if not output_path.is_absolute():
            output_path = (PROJECT_ROOT / output_path).resolve()

        self.output_path = output_path
        self.args = args
        self.config = config
        self.start_url_count = start_url_count
        self.stop_event = stop_event
        self.started_at_utc = utc_now_iso()
        self._started_monotonic = time.monotonic()
        self._lock = threading.Lock()
        self._flush_stop = threading.Event()
        self._flush_thread = threading.Thread(
            target=self._flush_loop,
            name="metrics-flush",
            daemon=True,
        )

        self._url_hits: Counter[str] = Counter()
        self._url_failures: Counter[str] = Counter()
        self._seen_urls: set[str] = set()
        self._seen_domains: set[str] = set()

        self._threads: dict[int, dict[str, Any]] = {
            worker_id: self._new_thread_row(worker_id) for worker_id in range(1, args.threads + 1)
        }

    def _new_thread_row(self, worker_id: int) -> dict[str, Any]:
        return {
            "worker_id": worker_id,
            "thread_name": f"worker-{worker_id}",
            "state": "starting",
            "alive": False,
            "thread_started_at_utc": None,
            "thread_stopped_at_utc": None,
            "last_activity_utc": None,
            "last_cycle_started_at_utc": None,
            "last_cycle_completed_at_utc": None,
            "last_seed_url": None,
            "last_url": None,
            "last_error": None,
            "last_status_code": None,
            "last_user_agent_profile": None,
            "last_accept_language": None,
            "cycles_started": 0,
            "cycles_completed": 0,
            "cycles_failed": 0,
            "click_goals_total": 0,
            "clicks_attempted": 0,
            "clicks_completed": 0,
            "internal_link_scans": 0,
            "internal_link_candidates": 0,
            "no_candidate_events": 0,
            "navigation_attempts": 0,
            "navigation_successes": 0,
            "navigation_failures": 0,
            "pages_opened": 0,
            "seed_pages_opened": 0,
            "internal_pages_opened": 0,
            "http_error_pages": 0,
            "navigation_elapsed_total_ms": 0.0,
            "navigation_elapsed_max_ms": 0.0,
            "page_browse_target_seconds": 0.0,
            "page_browse_actual_seconds": 0.0,
            "scroll_target_seconds": 0.0,
            "scroll_actual_seconds": 0.0,
            "scroll_actions": 0,
            "scroll_pixels": 0,
            "scroll_pause_seconds": 0.0,
            "post_scroll_delay_seconds": 0.0,
            "remainder_wait_seconds": 0.0,
            "comment_attempts": 0,
            "comment_forms_detected": 0,
            "comment_form_missing": 0,
            "comment_text_filled": 0,
            "comment_name_filled": 0,
            "comment_profile_anonymous": 0,
            "comment_profile_name": 0,
            "comment_publish_attempts": 0,
            "comment_publish_successes": 0,
            "comment_publish_failures": 0,
            "comment_captcha_detected": 0,
            "comment_skipped_by_rate": 0,
            "site_padding_seconds": 0.0,
            "cycle_elapsed_total_seconds": 0.0,
            "cycle_elapsed_max_seconds": 0.0,
            "timeouts": 0,
            "playwright_errors": 0,
            "unexpected_errors": 0,
            "status_counts": {"2xx": 0, "3xx": 0, "4xx": 0, "5xx": 0, "other": 0, "none": 0},
            "error_counts": {},
        }

    def start(self) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.flush()
        self._flush_thread.start()

    def stop(self) -> None:
        self._flush_stop.set()
        if self._flush_thread.is_alive():
            self._flush_thread.join(timeout=2.0)
        self.flush()

    def _flush_loop(self) -> None:
        while not self._flush_stop.wait(self.args.metrics_flush_seconds):
            self.flush()

    def _touch(self, row: dict[str, Any]) -> None:
        row["last_activity_utc"] = utc_now_iso()

    def _register_url(self, url: str | None) -> None:
        if not url:
            return
        self._seen_urls.add(url)
        host = urlparse(url).netloc
        if host:
            self._seen_domains.add(host)

    def _increment_error_kind(self, row: dict[str, Any], error_kind: str) -> None:
        error_counts = row.get("error_counts") or {}
        error_counts[error_kind] = int(error_counts.get(error_kind, 0)) + 1
        row["error_counts"] = error_counts

    def mark_worker_started(self, worker_id: int) -> None:
        with self._lock:
            row = self._threads[worker_id]
            row["alive"] = True
            row["state"] = "idle"
            row["thread_started_at_utc"] = row.get("thread_started_at_utc") or utc_now_iso()
            row["thread_stopped_at_utc"] = None
            self._touch(row)

    def mark_worker_stopped(self, worker_id: int) -> None:
        with self._lock:
            row = self._threads[worker_id]
            row["alive"] = False
            row["state"] = "stopped"
            row["thread_stopped_at_utc"] = utc_now_iso()
            self._touch(row)

    def mark_cycle_start(
        self,
        worker_id: int,
        seed_url: str,
        click_goal: int,
        profile_name: str,
        accept_language: str,
    ) -> None:
        with self._lock:
            row = self._threads[worker_id]
            row["state"] = "running_cycle"
            row["cycles_started"] += 1
            row["click_goals_total"] += max(0, click_goal)
            row["last_seed_url"] = seed_url
            row["last_url"] = seed_url
            row["last_user_agent_profile"] = profile_name
            row["last_accept_language"] = accept_language
            row["last_cycle_started_at_utc"] = utc_now_iso()
            self._register_url(seed_url)
            self._touch(row)

    def mark_cycle_end(self, worker_id: int, success: bool, elapsed_seconds: float) -> None:
        with self._lock:
            row = self._threads[worker_id]
            if success:
                row["cycles_completed"] += 1
            else:
                row["cycles_failed"] += 1
            row["cycle_elapsed_total_seconds"] += max(0.0, elapsed_seconds)
            row["cycle_elapsed_max_seconds"] = max(row["cycle_elapsed_max_seconds"], max(0.0, elapsed_seconds))
            row["last_cycle_completed_at_utc"] = utc_now_iso()
            row["state"] = "idle" if row.get("alive") else "stopped"
            self._touch(row)

    def record_navigation(
        self,
        worker_id: int,
        url: str,
        status_code: int | None,
        elapsed_seconds: float,
        is_seed: bool,
        ok: bool,
        error_message: str | None = None,
    ) -> None:
        with self._lock:
            row = self._threads[worker_id]
            row["navigation_attempts"] += 1
            elapsed_ms = max(0.0, elapsed_seconds) * 1000.0
            row["navigation_elapsed_total_ms"] += elapsed_ms
            row["navigation_elapsed_max_ms"] = max(row["navigation_elapsed_max_ms"], elapsed_ms)

            bucket = status_bucket(status_code)
            status_counts = row["status_counts"]
            status_counts[bucket] = int(status_counts.get(bucket, 0)) + 1
            row["status_counts"] = status_counts

            if ok:
                row["navigation_successes"] += 1
                row["pages_opened"] += 1
                if is_seed:
                    row["seed_pages_opened"] += 1
                else:
                    row["internal_pages_opened"] += 1
            else:
                row["navigation_failures"] += 1

            if status_code is not None and status_code >= 400:
                row["http_error_pages"] += 1

            if url:
                self._register_url(url)
                if ok:
                    self._url_hits[url] += 1
                if not ok or (status_code is not None and status_code >= 400):
                    self._url_failures[url] += 1

            row["last_url"] = url
            row["last_status_code"] = status_code
            if error_message:
                row["last_error"] = error_message
            self._touch(row)

    def record_link_scan(self, worker_id: int, candidate_count: int, no_candidates: bool) -> None:
        with self._lock:
            row = self._threads[worker_id]
            row["internal_link_scans"] += 1
            row["internal_link_candidates"] += max(0, candidate_count)
            if no_candidates:
                row["no_candidate_events"] += 1
            self._touch(row)

    def record_click_attempt(self, worker_id: int, url: str) -> None:
        with self._lock:
            row = self._threads[worker_id]
            row["clicks_attempted"] += 1
            row["last_url"] = url
            self._register_url(url)
            self._touch(row)

    def record_click_completed(self, worker_id: int, url: str) -> None:
        with self._lock:
            row = self._threads[worker_id]
            row["clicks_completed"] += 1
            row["last_url"] = url
            self._register_url(url)
            self._touch(row)

    def record_engagement(self, worker_id: int, stats: EngagementStats) -> None:
        with self._lock:
            row = self._threads[worker_id]
            row["page_browse_target_seconds"] += max(0.0, stats.page_browse_target_seconds)
            row["page_browse_actual_seconds"] += max(0.0, stats.page_browse_actual_seconds)
            row["scroll_target_seconds"] += max(0.0, stats.scroll_target_seconds)
            row["scroll_actual_seconds"] += max(0.0, stats.scroll_actual_seconds)
            row["scroll_actions"] += max(0, int(stats.scroll_actions))
            row["scroll_pixels"] += max(0, int(stats.scroll_pixels))
            row["scroll_pause_seconds"] += max(0.0, stats.scroll_pause_seconds)
            row["post_scroll_delay_seconds"] += max(0.0, stats.post_scroll_delay_seconds)
            row["remainder_wait_seconds"] += max(0.0, stats.remainder_wait_seconds)
            self._touch(row)

    def record_site_padding(self, worker_id: int, seconds: float) -> None:
        with self._lock:
            row = self._threads[worker_id]
            row["site_padding_seconds"] += max(0.0, seconds)
            self._touch(row)

    def record_comment_event(
        self,
        worker_id: int,
        profile: str | None = None,
        comment_attempted: bool = False,
        form_detected: bool = False,
        form_missing: bool = False,
        comment_text_filled: bool = False,
        comment_name_filled: bool = False,
        publish_attempted: bool = False,
        publish_success: bool = False,
        captcha_detected: bool = False,
        skipped_by_rate: bool = False,
        error_kind: str | None = None,
    ) -> None:
        with self._lock:
            row = self._threads[worker_id]
            if skipped_by_rate:
                row["comment_skipped_by_rate"] += 1
            if comment_attempted:
                row["comment_attempts"] += 1
            if form_detected:
                row["comment_forms_detected"] += 1
            if form_missing:
                row["comment_form_missing"] += 1
            if comment_text_filled:
                row["comment_text_filled"] += 1
            if comment_name_filled:
                row["comment_name_filled"] += 1
            if profile == "name":
                row["comment_profile_name"] += 1
            elif profile == "anonymous":
                row["comment_profile_anonymous"] += 1
            if publish_attempted:
                row["comment_publish_attempts"] += 1
            if publish_success:
                row["comment_publish_successes"] += 1
            if publish_attempted and not publish_success:
                row["comment_publish_failures"] += 1
            if captcha_detected:
                row["comment_captcha_detected"] += 1
            if error_kind:
                self._increment_error_kind(row, error_kind)
                row["last_error"] = error_kind
            self._touch(row)

    def record_error(self, worker_id: int, error_kind: str, message: str, url: str | None = None) -> None:
        with self._lock:
            row = self._threads[worker_id]
            if error_kind == "timeout":
                row["timeouts"] += 1
            elif error_kind == "playwright_error":
                row["playwright_errors"] += 1
            else:
                row["unexpected_errors"] += 1

            self._increment_error_kind(row, error_kind)
            row["last_error"] = message
            if url:
                row["last_url"] = url
                self._register_url(url)
                self._url_failures[url] += 1
            row["state"] = "error"
            self._touch(row)

    def _aggregate_status_counts(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        buckets = {"2xx": 0, "3xx": 0, "4xx": 0, "5xx": 0, "other": 0, "none": 0}
        for row in rows:
            for key, value in (row.get("status_counts") or {}).items():
                buckets[key] = int(buckets.get(key, 0)) + int(value)
        return buckets

    def _aggregate_error_counts(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        errors: dict[str, int] = {}
        for row in rows:
            for key, value in (row.get("error_counts") or {}).items():
                errors[key] = int(errors.get(key, 0)) + int(value)
        return dict(sorted(errors.items(), key=lambda item: item[1], reverse=True))

    def _build_summary(self, rows: list[dict[str, Any]], uptime_seconds: float) -> dict[str, Any]:
        def isum(field: str) -> int:
            return int(sum(int(row.get(field, 0) or 0) for row in rows))

        def fsum(field: str) -> float:
            return float(sum(float(row.get(field, 0.0) or 0.0) for row in rows))

        cycles_started = isum("cycles_started")
        cycles_completed = isum("cycles_completed")
        cycles_failed = isum("cycles_failed")
        clicks_attempted = isum("clicks_attempted")
        clicks_completed = isum("clicks_completed")
        navigation_attempts = isum("navigation_attempts")
        navigation_successes = isum("navigation_successes")
        navigation_failures = isum("navigation_failures")
        pages_opened = isum("pages_opened")
        navigation_elapsed_total_ms = fsum("navigation_elapsed_total_ms")
        cycle_elapsed_total_seconds = fsum("cycle_elapsed_total_seconds")

        per_minute_divisor = max(uptime_seconds / 60.0, 1e-9)
        comment_attempts = isum("comment_attempts")
        comment_publish_attempts = isum("comment_publish_attempts")
        comment_publish_successes = isum("comment_publish_successes")

        return {
            "threads_configured": self.args.threads,
            "threads_alive": sum(1 for row in rows if bool(row.get("alive"))),
            "active_cycles": sum(1 for row in rows if row.get("state") == "running_cycle"),
            "cycles_started": cycles_started,
            "cycles_completed": cycles_completed,
            "cycles_failed": cycles_failed,
            "cycle_success_rate_pct": round((cycles_completed / cycles_started * 100.0), 2) if cycles_started else 0.0,
            "click_goals_total": isum("click_goals_total"),
            "clicks_attempted": clicks_attempted,
            "clicks_completed": clicks_completed,
            "click_completion_rate_pct": round((clicks_completed / clicks_attempted * 100.0), 2)
            if clicks_attempted
            else 0.0,
            "internal_link_scans": isum("internal_link_scans"),
            "internal_link_candidates": isum("internal_link_candidates"),
            "no_candidate_events": isum("no_candidate_events"),
            "navigation_attempts": navigation_attempts,
            "navigation_successes": navigation_successes,
            "navigation_failures": navigation_failures,
            "navigation_success_rate_pct": round((navigation_successes / navigation_attempts * 100.0), 2)
            if navigation_attempts
            else 0.0,
            "pages_opened": pages_opened,
            "seed_pages_opened": isum("seed_pages_opened"),
            "internal_pages_opened": isum("internal_pages_opened"),
            "http_error_pages": isum("http_error_pages"),
            "navigation_avg_ms": round((navigation_elapsed_total_ms / navigation_attempts), 2) if navigation_attempts else 0.0,
            "navigation_max_ms": round(max((float(row.get("navigation_elapsed_max_ms", 0.0) or 0.0) for row in rows), default=0.0), 2),
            "page_browse_target_seconds": round(fsum("page_browse_target_seconds"), 3),
            "page_browse_actual_seconds": round(fsum("page_browse_actual_seconds"), 3),
            "scroll_target_seconds": round(fsum("scroll_target_seconds"), 3),
            "scroll_actual_seconds": round(fsum("scroll_actual_seconds"), 3),
            "scroll_actions": isum("scroll_actions"),
            "scroll_pixels": isum("scroll_pixels"),
            "scroll_pause_seconds": round(fsum("scroll_pause_seconds"), 3),
            "post_scroll_delay_seconds": round(fsum("post_scroll_delay_seconds"), 3),
            "remainder_wait_seconds": round(fsum("remainder_wait_seconds"), 3),
            "site_padding_seconds": round(fsum("site_padding_seconds"), 3),
            "comment_attempts": comment_attempts,
            "comment_forms_detected": isum("comment_forms_detected"),
            "comment_form_missing": isum("comment_form_missing"),
            "comment_text_filled": isum("comment_text_filled"),
            "comment_name_filled": isum("comment_name_filled"),
            "comment_profile_anonymous": isum("comment_profile_anonymous"),
            "comment_profile_name": isum("comment_profile_name"),
            "comment_publish_attempts": comment_publish_attempts,
            "comment_publish_successes": comment_publish_successes,
            "comment_publish_failures": isum("comment_publish_failures"),
            "comment_captcha_detected": isum("comment_captcha_detected"),
            "comment_skipped_by_rate": isum("comment_skipped_by_rate"),
            "comment_publish_success_rate_pct": round((comment_publish_successes / comment_publish_attempts * 100.0), 2)
            if comment_publish_attempts
            else 0.0,
            "cycle_elapsed_total_seconds": round(cycle_elapsed_total_seconds, 3),
            "cycle_elapsed_avg_seconds": round((cycle_elapsed_total_seconds / cycles_started), 3) if cycles_started else 0.0,
            "cycle_elapsed_max_seconds": round(max((float(row.get("cycle_elapsed_max_seconds", 0.0) or 0.0) for row in rows), default=0.0), 3),
            "timeouts": isum("timeouts"),
            "playwright_errors": isum("playwright_errors"),
            "unexpected_errors": isum("unexpected_errors"),
            "status_counts": self._aggregate_status_counts(rows),
            "error_counts": self._aggregate_error_counts(rows),
            "pages_per_minute": round(pages_opened / per_minute_divisor, 3),
            "cycles_per_minute": round(cycles_completed / per_minute_divisor, 3),
            "clicks_per_minute": round(clicks_completed / per_minute_divisor, 3),
            "unique_urls_seen": len(self._seen_urls),
            "unique_domains_seen": len(self._seen_domains),
            "top_visited_urls": [{"url": url, "hits": count} for url, count in self._url_hits.most_common(20)],
            "top_failed_urls": [{"url": url, "failures": count} for url, count in self._url_failures.most_common(20)],
            "uptime_seconds": round(uptime_seconds, 3),
        }

    def _build_payload(self) -> dict[str, Any]:
        with self._lock:
            rows = [dict(row) for _, row in sorted(self._threads.items(), key=lambda item: item[0])]
            for row in rows:
                row["status_counts"] = dict(row.get("status_counts") or {})
                row["error_counts"] = dict(row.get("error_counts") or {})

            uptime_seconds = max(0.0, time.monotonic() - self._started_monotonic)
            summary = self._build_summary(rows, uptime_seconds)

            return {
                "generated_at_utc": utc_now_iso(),
                "runner": {
                    "name": "threaded_browser_journey",
                    "version": "1.1",
                    "started_at_utc": self.started_at_utc,
                    "config_path": self.args.config,
                    "browser": self.args.browser,
                    "headless": self.args.headless,
                    "threads_configured": self.args.threads,
                    "min_clicks": self.args.min_clicks,
                    "max_clicks": self.args.max_clicks,
                    "min_page_browse_seconds": self.args.min_page_browse_seconds,
                    "max_page_browse_seconds": self.args.max_page_browse_seconds,
                    "min_scroll_seconds": self.args.min_scroll_seconds,
                    "max_scroll_seconds": self.args.max_scroll_seconds,
                    "min_scroll_pause_seconds": self.args.min_scroll_pause_seconds,
                    "max_scroll_pause_seconds": self.args.max_scroll_pause_seconds,
                    "min_post_scroll_click_delay_seconds": self.args.min_post_scroll_click_delay_seconds,
                    "max_post_scroll_click_delay_seconds": self.args.max_post_scroll_click_delay_seconds,
                    "min_site_browse_seconds": self.args.min_site_browse_seconds,
                    "max_site_browse_seconds": self.args.max_site_browse_seconds,
                    "scroll_step_px": self.args.scroll_step_px,
                    "disable_scroll": self.args.disable_scroll,
                    "navigation_timeout_seconds": self.args.navigation_timeout_seconds,
                    "max_cycles_per_thread": self.args.max_cycles_per_thread,
                    "metrics_output": str(self.output_path),
                    "metrics_flush_seconds": self.args.metrics_flush_seconds,
                    "enable_comments": self.args.enable_comments,
                    "comment_profile_mode": self.args.comment_profile_mode,
                    "comment_names_file": self.args.comment_names_file,
                    "comments_file": self.args.comments_file,
                    "comment_attempt_rate": self.args.comment_attempt_rate,
                    "comment_simple_mode": self.args.comment_simple_mode,
                    "comment_min_wait_seconds": self.args.comment_min_wait_seconds,
                    "comment_max_wait_seconds": self.args.comment_max_wait_seconds,
                    "stop_requested": self.stop_event.is_set(),
                    "target_count": len(self.config.targets),
                    "traffic_profile_count": len(self.config.traffic_profiles),
                    "available_start_urls": self.start_url_count,
                },
                "summary": summary,
                "threads": rows,
            }

    def flush(self) -> None:
        payload = self._build_payload()
        tmp = self.output_path.with_suffix(self.output_path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp.replace(self.output_path)


def ensure_playwright_installed(browser_name: str) -> None:
    ok, message = ensure_playwright_browsers([browser_name], install_missing=True)
    if not ok:
        raise SystemExit(message)
    if message and "installed" in message.lower():
        logging.info(message)


def parse_args() -> RunnerArgs:
    parser = argparse.ArgumentParser(
        description=(
            "Run threaded browser journeys: each thread opens a random URL, "
            "browses 3-4 internal links, waits 20-30 seconds between pages, "
            "then repeats with another random URL."
        )
    )
    parser.add_argument("--config", default="config/sites.yaml")
    parser.add_argument("--threads", type=int, default=5)
    parser.add_argument("--browser", choices=["chromium", "firefox", "webkit"], default="chromium")
    parser.add_argument("--min-clicks", type=int, default=3)
    parser.add_argument("--max-clicks", type=int, default=4)
    parser.add_argument(
        "--min-page-browse-seconds",
        "--min-dwell-seconds",
        dest="min_page_browse_seconds",
        type=float,
        default=20.0,
    )
    parser.add_argument(
        "--max-page-browse-seconds",
        "--max-dwell-seconds",
        dest="max_page_browse_seconds",
        type=float,
        default=30.0,
    )
    parser.add_argument("--min-scroll-seconds", type=float, default=6.0)
    parser.add_argument("--max-scroll-seconds", type=float, default=12.0)
    parser.add_argument("--min-scroll-pause-seconds", type=float, default=1.0)
    parser.add_argument("--max-scroll-pause-seconds", type=float, default=3.0)
    parser.add_argument("--min-post-scroll-click-delay-seconds", type=float, default=2.0)
    parser.add_argument("--max-post-scroll-click-delay-seconds", type=float, default=5.0)
    parser.add_argument(
        "--min-site-browse-seconds",
        type=float,
        default=0.0,
        help="Minimum total seconds to spend in one site cycle before switching to a new seed URL",
    )
    parser.add_argument(
        "--max-site-browse-seconds",
        type=float,
        default=0.0,
        help="Maximum total seconds to spend in one site cycle before switching to a new seed URL",
    )
    parser.add_argument("--scroll-step-px", type=int, default=700)
    parser.add_argument("--disable-scroll", action="store_true")
    parser.add_argument("--navigation-timeout-seconds", type=float, default=45.0)
    parser.add_argument(
        "--max-cycles-per-thread",
        type=int,
        default=0,
        help="0 means run continuously until stopped",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run in headed mode (default is headless)",
    )
    parser.add_argument(
        "--metrics-output",
        default=str(PROJECT_ROOT / "results" / "threaded_journey_metrics.json"),
        help="JSON output path for threaded runner metrics",
    )
    parser.add_argument(
        "--metrics-flush-seconds",
        type=float,
        default=2.0,
        help="How frequently metrics are flushed to JSON",
    )
    parser.add_argument(
        "--enable-comments",
        action="store_true",
        help="Attempt comment posting on visited pages using provided comment pools",
    )
    parser.add_argument(
        "--comment-profile-mode",
        choices=["anonymous", "name", "mixed"],
        default="anonymous",
        help="Comment profile behavior to use when posting comments",
    )
    parser.add_argument(
        "--comment-names-file",
        default="",
        help="Path to names file (.txt/.rtf/.csv/.json) used for name-based comments",
    )
    parser.add_argument(
        "--comments-file",
        default="",
        help="Path to comments file (.txt/.csv/.json) used as the comment pool",
    )
    parser.add_argument(
        "--comment-attempt-rate",
        type=float,
        default=1.0,
        help="0.0-1.0 probability to attempt posting a comment on each visited page",
    )
    parser.add_argument(
        "--comment-simple-mode",
        action="store_true",
        help="Simple comment journey: open target URL, wait, post comment, move to next URL",
    )
    parser.add_argument(
        "--comment-min-wait-seconds",
        type=float,
        default=5.0,
        help="Minimum seconds to wait on page before posting a comment in comment-simple-mode",
    )
    parser.add_argument(
        "--comment-max-wait-seconds",
        type=float,
        default=10.0,
        help="Maximum seconds to wait on page before posting a comment in comment-simple-mode",
    )
    parsed = parser.parse_args()

    if parsed.threads < 1:
        raise SystemExit("--threads must be >= 1")
    if parsed.min_clicks < 1 or parsed.max_clicks < parsed.min_clicks:
        raise SystemExit("Invalid click bounds")
    if parsed.min_page_browse_seconds < 0 or parsed.max_page_browse_seconds < parsed.min_page_browse_seconds:
        raise SystemExit("Invalid page browse duration bounds")
    if parsed.min_scroll_seconds < 0 or parsed.max_scroll_seconds < parsed.min_scroll_seconds:
        raise SystemExit("Invalid scroll duration bounds")
    if parsed.min_scroll_pause_seconds < 0 or parsed.max_scroll_pause_seconds < parsed.min_scroll_pause_seconds:
        raise SystemExit("Invalid scroll pause bounds")
    if (
        parsed.min_post_scroll_click_delay_seconds < 0
        or parsed.max_post_scroll_click_delay_seconds < parsed.min_post_scroll_click_delay_seconds
    ):
        raise SystemExit("Invalid post-scroll click delay bounds")
    if parsed.min_site_browse_seconds < 0 or parsed.max_site_browse_seconds < parsed.min_site_browse_seconds:
        raise SystemExit("Invalid site browse duration bounds")
    if parsed.scroll_step_px <= 0:
        raise SystemExit("--scroll-step-px must be > 0")
    if parsed.navigation_timeout_seconds <= 0:
        raise SystemExit("--navigation-timeout-seconds must be > 0")
    if parsed.metrics_flush_seconds <= 0:
        raise SystemExit("--metrics-flush-seconds must be > 0")
    if parsed.comment_attempt_rate < 0 or parsed.comment_attempt_rate > 1:
        raise SystemExit("--comment-attempt-rate must be between 0 and 1")
    if parsed.comment_min_wait_seconds < 0 or parsed.comment_max_wait_seconds < parsed.comment_min_wait_seconds:
        raise SystemExit("Invalid comment wait duration bounds")
    if parsed.enable_comments and not str(parsed.comments_file).strip():
        raise SystemExit("--comments-file is required when --enable-comments is set")
    if parsed.comment_simple_mode and not parsed.enable_comments:
        raise SystemExit("--comment-simple-mode requires --enable-comments")
    if (
        parsed.enable_comments
        and parsed.comment_profile_mode in {"name", "mixed"}
        and not str(parsed.comment_names_file).strip()
    ):
        raise SystemExit("--comment-names-file is required for comment profile mode name/mixed")

    return RunnerArgs(
        config=parsed.config,
        threads=parsed.threads,
        browser=parsed.browser,
        min_clicks=parsed.min_clicks,
        max_clicks=parsed.max_clicks,
        min_page_browse_seconds=parsed.min_page_browse_seconds,
        max_page_browse_seconds=parsed.max_page_browse_seconds,
        min_scroll_seconds=parsed.min_scroll_seconds,
        max_scroll_seconds=parsed.max_scroll_seconds,
        min_scroll_pause_seconds=parsed.min_scroll_pause_seconds,
        max_scroll_pause_seconds=parsed.max_scroll_pause_seconds,
        min_post_scroll_click_delay_seconds=parsed.min_post_scroll_click_delay_seconds,
        max_post_scroll_click_delay_seconds=parsed.max_post_scroll_click_delay_seconds,
        min_site_browse_seconds=parsed.min_site_browse_seconds,
        max_site_browse_seconds=parsed.max_site_browse_seconds,
        scroll_step_px=parsed.scroll_step_px,
        disable_scroll=parsed.disable_scroll,
        navigation_timeout_seconds=parsed.navigation_timeout_seconds,
        max_cycles_per_thread=parsed.max_cycles_per_thread,
        headless=not parsed.headed,
        metrics_output=parsed.metrics_output,
        metrics_flush_seconds=parsed.metrics_flush_seconds,
        enable_comments=parsed.enable_comments,
        comment_profile_mode=parsed.comment_profile_mode,
        comment_names_file=parsed.comment_names_file,
        comments_file=parsed.comments_file,
        comment_attempt_rate=parsed.comment_attempt_rate,
        comment_simple_mode=parsed.comment_simple_mode,
        comment_min_wait_seconds=parsed.comment_min_wait_seconds,
        comment_max_wait_seconds=parsed.comment_max_wait_seconds,
    )


def strip_fragment(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse(parsed._replace(fragment=""))


def build_start_urls(config: TestConfig) -> list[str]:
    urls: set[str] = set()
    for target in config.targets:
        for path in target.paths + target.query_paths:
            path_part = path if path.startswith("/") else f"/{path}"
            urls.add(f"{target.base_url}{path_part}")
    return sorted(urls)


def sleep_interruptible(seconds: float, stop_event: threading.Event) -> float:
    target_seconds = max(0.0, seconds)
    if target_seconds <= 0:
        return 0.0

    started = time.monotonic()
    deadline = started + target_seconds
    while not stop_event.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(0.5, remaining))
    return max(0.0, time.monotonic() - started)


def select_internal_links(page_url: str, hrefs: list[str], max_links: int, rng: random.Random) -> list[str]:
    host = urlparse(page_url).netloc
    selected: list[str] = []
    seen: set[str] = set()
    for href in hrefs:
        if not href:
            continue
        parsed = urlparse(href)
        if parsed.scheme not in {"http", "https"}:
            continue
        if parsed.netloc != host:
            continue
        normalized = strip_fragment(href)
        if normalized in seen:
            continue
        seen.add(normalized)
        selected.append(normalized)

    rng.shuffle(selected)
    return selected[:max_links]


def choose_traffic_profile(rng: random.Random, configured_profiles: list[TrafficProfile]) -> TrafficProfile:
    profile_pool = configured_profiles if len(configured_profiles) >= 25 else TOP_25_USER_AGENTS
    weights = [max(1, profile.weight) for profile in profile_pool]
    return rng.choices(profile_pool, weights=weights, k=1)[0]


def scroll_page_for_duration(
    page,
    args: RunnerArgs,
    scroll_seconds: float,
    stop_event: threading.Event,
    rng: random.Random,
) -> tuple[float, int, int, float]:
    if args.disable_scroll or scroll_seconds <= 0:
        return 0.0, 0, 0, 0.0

    started = time.monotonic()
    deadline = started + scroll_seconds
    direction = 1
    actions = 0
    pixels = 0
    pause_seconds = 0.0

    while not stop_event.is_set() and time.monotonic() < deadline:
        step = int(rng.uniform(0.65, 1.35) * args.scroll_step_px)
        delta = step * direction
        scroll_state = page.evaluate(
            """
            ({delta}) => {
                const maxY = Math.max(0, document.documentElement.scrollHeight - window.innerHeight);
                const y = window.scrollY || window.pageYOffset || 0;
                const nearBottom = y >= maxY - 120;
                const nearTop = y <= 20;
                let actual = delta;
                if (nearBottom && delta > 0) actual = -Math.max(160, Math.floor(delta * 0.6));
                if (nearTop && delta < 0) actual = Math.max(160, Math.floor(Math.abs(delta) * 0.6));
                window.scrollBy({ top: actual, left: 0, behavior: "smooth" });
                return { actual };
            }
            """,
            {"delta": delta},
        )
        actual_delta = int((scroll_state or {}).get("actual", delta))
        pixels += abs(actual_delta)
        actions += 1

        if rng.random() < 0.25:
            direction *= -1

        pause_target = rng.uniform(args.min_scroll_pause_seconds, args.max_scroll_pause_seconds)
        pause_seconds += sleep_interruptible(pause_target, stop_event)

    return max(0.0, time.monotonic() - started), actions, pixels, pause_seconds


def engage_page_before_next_click(
    page,
    args: RunnerArgs,
    stop_event: threading.Event,
    rng: random.Random,
) -> EngagementStats:
    page_browse_seconds = rng.uniform(args.min_page_browse_seconds, args.max_page_browse_seconds)
    scroll_seconds = min(
        page_browse_seconds,
        rng.uniform(args.min_scroll_seconds, args.max_scroll_seconds),
    )
    delay_after_scroll = rng.uniform(
        args.min_post_scroll_click_delay_seconds,
        args.max_post_scroll_click_delay_seconds,
    )

    stats = EngagementStats(
        page_browse_target_seconds=page_browse_seconds,
        scroll_target_seconds=scroll_seconds,
    )

    started = time.monotonic()
    scroll_elapsed, scroll_actions, scroll_pixels, scroll_pause_seconds = scroll_page_for_duration(
        page=page,
        args=args,
        scroll_seconds=scroll_seconds,
        stop_event=stop_event,
        rng=rng,
    )
    stats.scroll_actual_seconds = scroll_elapsed
    stats.scroll_actions = scroll_actions
    stats.scroll_pixels = scroll_pixels
    stats.scroll_pause_seconds = scroll_pause_seconds

    stats.post_scroll_delay_seconds = sleep_interruptible(delay_after_scroll, stop_event)

    spent = time.monotonic() - started
    remaining = page_browse_seconds - spent
    if remaining > 0:
        stats.remainder_wait_seconds = sleep_interruptible(remaining, stop_event)

    stats.page_browse_actual_seconds = max(0.0, time.monotonic() - started)
    return stats


def goto_with_metrics(
    page,
    url: str,
    args: RunnerArgs,
    metrics: ThreadMetricsCollector,
    worker_id: int,
    is_seed: bool,
) -> int | None:
    timeout_ms = int(args.navigation_timeout_seconds * 1000)
    started = time.monotonic()
    try:
        response = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    except Exception as exc:
        metrics.record_navigation(
            worker_id=worker_id,
            url=url,
            status_code=None,
            elapsed_seconds=time.monotonic() - started,
            is_seed=is_seed,
            ok=False,
            error_message=str(exc),
        )
        raise

    status_code = int(response.status) if response is not None else None
    metrics.record_navigation(
        worker_id=worker_id,
        url=url,
        status_code=status_code,
        elapsed_seconds=time.monotonic() - started,
        is_seed=is_seed,
        ok=True,
    )
    return status_code


def run_journey_cycle(
    page,
    worker_id: int,
    seed_url: str,
    profile_name: str,
    accept_language: str,
    args: RunnerArgs,
    metrics: ThreadMetricsCollector,
    stop_event: threading.Event,
    rng: random.Random,
    comment_engine: CommentEngine,
) -> None:
    if args.comment_simple_mode and comment_engine.enabled:
        run_comment_cycle_simple(
            page=page,
            worker_id=worker_id,
            seed_url=seed_url,
            profile_name=profile_name,
            accept_language=accept_language,
            args=args,
            metrics=metrics,
            stop_event=stop_event,
            rng=rng,
            comment_engine=comment_engine,
        )
        return

    click_goal = rng.randint(args.min_clicks, args.max_clicks)
    site_browse_target = 0.0
    if args.max_site_browse_seconds > 0:
        site_browse_target = rng.uniform(
            args.min_site_browse_seconds,
            args.max_site_browse_seconds,
        )

    cycle_started = time.monotonic()
    success = False
    metrics.mark_cycle_start(
        worker_id=worker_id,
        seed_url=seed_url,
        click_goal=click_goal,
        profile_name=profile_name,
        accept_language=accept_language,
    )

    try:
        goto_with_metrics(
            page=page,
            url=seed_url,
            args=args,
            metrics=metrics,
            worker_id=worker_id,
            is_seed=True,
        )
        logging.info("Opened seed URL: %s", seed_url)

        engagement = engage_page_before_next_click(page=page, args=args, stop_event=stop_event, rng=rng)
        metrics.record_engagement(worker_id=worker_id, stats=engagement)
        post_comment_on_page(
            page=page,
            worker_id=worker_id,
            args=args,
            comment_engine=comment_engine,
            metrics=metrics,
            stop_event=stop_event,
            rng=rng,
        )

        visited = {strip_fragment(seed_url)}
        clicks_done = 0

        while not stop_event.is_set() and clicks_done < click_goal:
            hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
            candidates = select_internal_links(page.url or seed_url, hrefs, max_links=20, rng=rng)
            candidates = [url for url in candidates if url not in visited]
            metrics.record_link_scan(worker_id=worker_id, candidate_count=len(candidates), no_candidates=not candidates)
            if not candidates:
                logging.info("No additional internal links available from %s", page.url or seed_url)
                break

            next_url = rng.choice(candidates)
            visited.add(next_url)
            metrics.record_click_attempt(worker_id=worker_id, url=next_url)

            goto_with_metrics(
                page=page,
                url=next_url,
                args=args,
                metrics=metrics,
                worker_id=worker_id,
                is_seed=False,
            )
            clicks_done += 1
            metrics.record_click_completed(worker_id=worker_id, url=next_url)

            logging.info("Browsed %d/%d: %s", clicks_done, click_goal, next_url)
            engagement = engage_page_before_next_click(page=page, args=args, stop_event=stop_event, rng=rng)
            metrics.record_engagement(worker_id=worker_id, stats=engagement)
            post_comment_on_page(
                page=page,
                worker_id=worker_id,
                args=args,
                comment_engine=comment_engine,
                metrics=metrics,
                stop_event=stop_event,
                rng=rng,
            )

        if site_browse_target > 0:
            elapsed = time.monotonic() - cycle_started
            if elapsed < site_browse_target:
                padding = sleep_interruptible(site_browse_target - elapsed, stop_event)
                metrics.record_site_padding(worker_id=worker_id, seconds=padding)

        success = True
    finally:
        metrics.mark_cycle_end(
            worker_id=worker_id,
            success=success,
            elapsed_seconds=max(0.0, time.monotonic() - cycle_started),
        )


def run_comment_cycle_simple(
    page,
    worker_id: int,
    seed_url: str,
    profile_name: str,
    accept_language: str,
    args: RunnerArgs,
    metrics: ThreadMetricsCollector,
    stop_event: threading.Event,
    rng: random.Random,
    comment_engine: CommentEngine,
) -> None:
    wait_target_seconds = rng.uniform(args.comment_min_wait_seconds, args.comment_max_wait_seconds)
    cycle_started = time.monotonic()
    success = False
    metrics.mark_cycle_start(
        worker_id=worker_id,
        seed_url=seed_url,
        click_goal=0,
        profile_name=profile_name,
        accept_language=accept_language,
    )

    try:
        goto_with_metrics(
            page=page,
            url=seed_url,
            args=args,
            metrics=metrics,
            worker_id=worker_id,
            is_seed=True,
        )
        logging.info("Opened comment target URL: %s", seed_url)

        waited_seconds = sleep_interruptible(wait_target_seconds, stop_event)
        metrics.record_engagement(
            worker_id=worker_id,
            stats=EngagementStats(
                page_browse_target_seconds=wait_target_seconds,
                page_browse_actual_seconds=waited_seconds,
                remainder_wait_seconds=waited_seconds,
            ),
        )

        post_comment_on_page(
            page=page,
            worker_id=worker_id,
            args=args,
            comment_engine=comment_engine,
            metrics=metrics,
            stop_event=stop_event,
            rng=rng,
        )
        success = True
    finally:
        metrics.mark_cycle_end(
            worker_id=worker_id,
            success=success,
            elapsed_seconds=max(0.0, time.monotonic() - cycle_started),
        )


def worker_main(
    worker_id: int,
    allocator: UrlAllocator,
    config: TestConfig,
    args: RunnerArgs,
    metrics: ThreadMetricsCollector,
    stop_event: threading.Event,
    comment_engine: CommentEngine,
) -> None:
    try:
        from playwright.sync_api import Error as PlaywrightError
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Playwright is not installed. Run: pip install -r requirements.txt && playwright install"
        ) from exc

    rng = random.Random((worker_id * 1_000_003) + int(time.time() * 1000))
    cycles = 0
    metrics.mark_worker_started(worker_id)

    try:
        with sync_playwright() as playwright:
            browser_type = getattr(playwright, args.browser)
            browser = browser_type.launch(headless=args.headless)
            try:
                while not stop_event.is_set():
                    if args.max_cycles_per_thread and cycles >= args.max_cycles_per_thread:
                        break

                    seed_url = allocator.acquire(rng)
                    profile = choose_traffic_profile(rng, config.traffic_profiles)
                    accept_language = rng.choice(config.headers_pool.accept_language)
                    context = None

                    try:
                        context = browser.new_context(
                            user_agent=profile.user_agent,
                            ignore_https_errors=not config.load_profile.ssl_verify,
                            locale=accept_language.split(",")[0],
                            extra_http_headers={"Accept-Language": accept_language},
                        )
                        page = context.new_page()
                        logging.info(
                            "Cycle %d start | seed=%s | ua=%s",
                            cycles + 1,
                            seed_url,
                            profile.name,
                        )
                        run_journey_cycle(
                            page=page,
                            worker_id=worker_id,
                            seed_url=seed_url,
                            profile_name=profile.name,
                            accept_language=accept_language,
                            args=args,
                            metrics=metrics,
                            stop_event=stop_event,
                            rng=rng,
                            comment_engine=comment_engine,
                        )
                    except PlaywrightTimeoutError as exc:
                        metrics.record_error(worker_id, "timeout", str(exc), url=seed_url)
                        logging.warning("Navigation timeout on %s: %s", seed_url, exc)
                    except PlaywrightError as exc:
                        metrics.record_error(worker_id, "playwright_error", str(exc), url=seed_url)
                        logging.warning("Browser error on %s: %s", seed_url, exc)
                    except Exception as exc:
                        metrics.record_error(worker_id, "unexpected_error", str(exc), url=seed_url)
                        logging.exception("Unexpected worker error on %s: %s", seed_url, exc)
                    finally:
                        if context is not None:
                            try:
                                context.close()
                            except PlaywrightError as exc:
                                message = str(exc)
                                if "has been closed" in message.lower() or "target closed" in message.lower():
                                    logging.debug("Context already closed for %s", seed_url)
                                else:
                                    metrics.record_error(worker_id, "context_close_error", message, url=seed_url)
                            except Exception as exc:  # noqa: BLE001
                                metrics.record_error(worker_id, "context_close_error", str(exc), url=seed_url)
                        allocator.release(seed_url)

                    cycles += 1
            finally:
                try:
                    browser.close()
                except PlaywrightError as exc:
                    message = str(exc)
                    if "has been closed" in message.lower() or "target closed" in message.lower():
                        logging.debug("Browser already closed for worker %d", worker_id)
                    else:
                        metrics.record_error(worker_id, "browser_close_error", message)
                except Exception as exc:  # noqa: BLE001
                    metrics.record_error(worker_id, "browser_close_error", str(exc))
    except Exception as exc:
        metrics.record_error(worker_id, "worker_fatal", str(exc))
        logging.exception("Worker %d stopped due to fatal error: %s", worker_id, exc)
    finally:
        metrics.mark_worker_stopped(worker_id)

    logging.info("Worker %d exiting after %d cycles", worker_id, cycles)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    args = parse_args()
    ensure_playwright_installed(args.browser)
    try:
        config = load_test_config(args.config)
    except ConfigError as exc:
        raise SystemExit(f"Invalid config: {exc}") from exc

    if not config.owner_authorization:
        raise SystemExit(
            "owner_authorization must be true in config before running threaded browser journeys."
        )

    comment_engine = build_comment_engine(args)
    if comment_engine.enabled:
        if args.comment_simple_mode:
            logging.info(
                "Comment simple mode enabled | profile=%s | comments=%d | names=%d | wait=%.1f-%.1fs | attempt_rate=%.2f",
                comment_engine.profile_mode,
                len(comment_engine.comments or []),
                len(comment_engine.names or []),
                args.comment_min_wait_seconds,
                args.comment_max_wait_seconds,
                comment_engine.comment_attempt_rate,
            )
        else:
            logging.info(
                "Comment posting enabled | profile=%s | comments=%d | names=%d | attempt_rate=%.2f",
                comment_engine.profile_mode,
                len(comment_engine.comments or []),
                len(comment_engine.names or []),
                comment_engine.comment_attempt_rate,
            )

    start_urls = build_start_urls(config)
    if len(start_urls) < args.threads:
        logging.warning(
            "Only %d unique start URLs available for %d threads. URL reuse may occur.",
            len(start_urls),
            args.threads,
        )

    allocator = UrlAllocator(start_urls)
    stop_event = threading.Event()
    metrics = ThreadMetricsCollector(
        args=args,
        config=config,
        start_url_count=len(start_urls),
        stop_event=stop_event,
    )
    metrics.start()
    logging.info("Threaded metrics JSON: %s", metrics.output_path)

    def _request_stop(*_args) -> None:
        logging.info("Stop requested. Shutting down workers...")
        stop_event.set()

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    workers: list[threading.Thread] = []

    try:
        for idx in range(args.threads):
            thread = threading.Thread(
                target=worker_main,
                name=f"worker-{idx + 1}",
                args=(idx + 1, allocator, config, args, metrics, stop_event, comment_engine),
                daemon=False,
            )
            workers.append(thread)
            thread.start()

        for thread in workers:
            thread.join()
    finally:
        stop_event.set()
        metrics.stop()

    logging.info("All workers stopped.")


if __name__ == "__main__":
    main()
