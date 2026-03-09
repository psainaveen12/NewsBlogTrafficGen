from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from loadtest.config import ConfigError, load_test_config
from loadtest.config_builder import build_default_config, build_targets, load_urls_from_files
from loadtest.devices import DEFAULT_BROWSER_MATRIX_DEVICES
from loadtest.runner_manager import (
    get_all_runner_statuses,
    read_log_tail,
    start_runner,
    stop_runner,
)

CONFIG_PRIMARY_PATH = PROJECT_ROOT / "config" / "sites.yaml"
CONFIG_EXAMPLE_PATH = PROJECT_ROOT / "config" / "sites.example.yaml"
RESULTS_DIR = PROJECT_ROOT / "results"
STATE_FILE = RESULTS_DIR / "dashboard_runtime.json"
THREAD_RUNNER_NAME = "threaded_journey"
COMMENT_RUNNER_NAME = "comment_journey"
LOCUST_RUNNER_NAME = "locust_headless"
BROWSER_MATRIX_RUNNER_NAME = "browser_matrix"
THREAD_LOG_FILE = RESULTS_DIR / "threaded_journey.log"
COMMENT_LOG_FILE = RESULTS_DIR / "comment_journey.log"
LOCUST_LOG_FILE = RESULTS_DIR / "locust_headless.log"
BROWSER_MATRIX_LOG_FILE = RESULTS_DIR / "browser_matrix.log"
BROWSER_MATRIX_OUTPUT_JSON = RESULTS_DIR / "browser-matrix.json"
THREAD_METRICS_FILE = RESULTS_DIR / "threaded_journey_metrics.json"
COMMENT_METRICS_FILE = RESULTS_DIR / "comment_journey_metrics.json"
THREADED_UI_CONFIG_FILE = CONFIG_PRIMARY_PATH.parent / "threaded_runner_ui.json"
COMMENT_UI_CONFIG_FILE = CONFIG_PRIMARY_PATH.parent / "comment_runner_ui.json"
THREADED_UI_DEFAULTS: dict[str, Any] = {
    "thr_threads": 5,
    "thr_browser": "chromium",
    "thr_min_clicks": 3,
    "thr_max_clicks": 4,
    "thr_cycle_limit": 0,
    "thr_nav_timeout": 45.0,
    "thr_headed": False,
    "thr_metrics_output": str(THREAD_METRICS_FILE),
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
COMMENT_UI_DEFAULTS: dict[str, Any] = {
    "com_threads": 5,
    "com_browser": "chromium",
    "com_cycle_limit": 0,
    "com_nav_timeout": 45.0,
    "com_headed": False,
    "com_metrics_output": str(COMMENT_METRICS_FILE),
    "com_metrics_flush": 2.0,
    "com_min_wait_seconds": 5.0,
    "com_max_wait_seconds": 10.0,
    "com_comment_profile_mode": "anonymous",
    "com_comment_names_file": str(PROJECT_ROOT / "config" / "1000_random_names.txt"),
    "com_comments_file": str(PROJECT_ROOT / "config" / "comments_10000.txt"),
}


def _normalize_runtime_path(value: Any, fallback: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return fallback
    if raw.startswith("/Users/"):
        return fallback
    if len(raw) > 2 and raw[1:3] == ":\\":
        return fallback
    return raw


def active_config_path() -> Path:
    if CONFIG_PRIMARY_PATH.exists():
        return CONFIG_PRIMARY_PATH
    if CONFIG_EXAMPLE_PATH.exists():
        return CONFIG_EXAMPLE_PATH
    return CONFIG_PRIMARY_PATH


def writable_config_path() -> Path:
    if CONFIG_PRIMARY_PATH.exists():
        return CONFIG_PRIMARY_PATH
    if CONFIG_EXAMPLE_PATH.exists():
        try:
            CONFIG_PRIMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
            CONFIG_PRIMARY_PATH.write_text(CONFIG_EXAMPLE_PATH.read_text(encoding="utf-8"), encoding="utf-8")
            return CONFIG_PRIMARY_PATH
        except Exception:
            return CONFIG_EXAMPLE_PATH
    return CONFIG_PRIMARY_PATH



def load_threaded_ui_config() -> dict[str, Any]:
    defaults = dict(THREADED_UI_DEFAULTS)
    if not THREADED_UI_CONFIG_FILE.exists():
        return defaults

    try:
        raw = json.loads(THREADED_UI_CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception:
        return defaults

    if not isinstance(raw, dict):
        return defaults

    loaded = dict(defaults)
    for key, default in defaults.items():
        value = raw.get(key, default)
        try:
            if isinstance(default, bool):
                loaded[key] = bool(value)
            elif isinstance(default, int):
                loaded[key] = int(value)
            elif isinstance(default, float):
                loaded[key] = float(value)
            else:
                loaded[key] = str(value)
        except (TypeError, ValueError):
            loaded[key] = default

    if loaded["thr_browser"] not in {"chromium", "firefox", "webkit"}:
        loaded["thr_browser"] = "chromium"
    loaded["thr_metrics_output"] = _normalize_runtime_path(
        loaded.get("thr_metrics_output"),
        str(THREAD_METRICS_FILE),
    )

    return loaded


def save_threaded_ui_config_from_session() -> None:
    payload: dict[str, Any] = {}
    for key, default in THREADED_UI_DEFAULTS.items():
        value = st.session_state.get(key, default)
        if isinstance(default, bool):
            payload[key] = bool(value)
        elif isinstance(default, int):
            payload[key] = int(value)
        elif isinstance(default, float):
            payload[key] = float(value)
        else:
            payload[key] = str(value)

    THREADED_UI_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    THREADED_UI_CONFIG_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_comment_ui_config() -> dict[str, Any]:
    defaults = dict(COMMENT_UI_DEFAULTS)
    if not COMMENT_UI_CONFIG_FILE.exists():
        return defaults

    try:
        raw = json.loads(COMMENT_UI_CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception:
        return defaults

    if not isinstance(raw, dict):
        return defaults

    loaded = dict(defaults)
    for key, default in defaults.items():
        value = raw.get(key, default)
        try:
            if isinstance(default, bool):
                loaded[key] = bool(value)
            elif isinstance(default, int):
                loaded[key] = int(value)
            elif isinstance(default, float):
                loaded[key] = float(value)
            else:
                loaded[key] = str(value)
        except (TypeError, ValueError):
            loaded[key] = default

    if loaded["com_browser"] not in {"chromium", "firefox", "webkit"}:
        loaded["com_browser"] = "chromium"
    if loaded["com_comment_profile_mode"] not in {"anonymous", "name", "mixed"}:
        loaded["com_comment_profile_mode"] = "anonymous"
    loaded["com_metrics_output"] = _normalize_runtime_path(
        loaded.get("com_metrics_output"),
        str(COMMENT_METRICS_FILE),
    )
    loaded["com_comment_names_file"] = _normalize_runtime_path(
        loaded.get("com_comment_names_file"),
        str(PROJECT_ROOT / "config" / "1000_random_names.txt"),
    )
    loaded["com_comments_file"] = _normalize_runtime_path(
        loaded.get("com_comments_file"),
        str(PROJECT_ROOT / "config" / "comments_10000.txt"),
    )

    return loaded


def save_comment_ui_config_from_session() -> None:
    payload: dict[str, Any] = {}
    for key, default in COMMENT_UI_DEFAULTS.items():
        value = st.session_state.get(key, default)
        if isinstance(default, bool):
            payload[key] = bool(value)
        elif isinstance(default, int):
            payload[key] = int(value)
        elif isinstance(default, float):
            payload[key] = float(value)
        else:
            payload[key] = str(value)

    COMMENT_UI_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    COMMENT_UI_CONFIG_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")

def apply_style() -> None:
    css_path = PROJECT_ROOT / "dashboard" / "styles.css"
    if css_path.exists():
        st.markdown(f"<style>{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)


def metric_card(title: str, value: str, subtitle: str) -> None:
    st.markdown(
        f"""
<div class="metric-card">
  <div class="metric-title">{title}</div>
  <div class="metric-value">{value}</div>
  <div class="metric-subtitle">{subtitle}</div>
</div>
""",
        unsafe_allow_html=True,
    )


def load_config_text() -> str:
    config_path = active_config_path()
    if not config_path.exists():
        return ""
    return config_path.read_text(encoding="utf-8")


def save_config_from_text(yaml_text: str) -> dict[str, Any]:
    parsed = yaml.safe_load(yaml_text)
    if not isinstance(parsed, dict):
        raise ValueError("Config YAML must define a top-level mapping")
    config_path = writable_config_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml.safe_dump(parsed, sort_keys=False), encoding="utf-8")
    return parsed


def set_owner_authorization(enabled: bool) -> dict[str, Any]:
    raw_text = load_config_text()
    parsed = yaml.safe_load(raw_text) if raw_text.strip() else {}
    if not isinstance(parsed, dict):
        raise ValueError("Config YAML must define a top-level mapping")
    parsed["owner_authorization"] = bool(enabled)
    config_path = writable_config_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml.safe_dump(parsed, sort_keys=False), encoding="utf-8")
    return parsed


def validate_current_config() -> tuple[bool, str]:
    try:
        cfg = load_test_config(active_config_path())
    except Exception as exc:
        return False, str(exc)
    total_paths = sum(len(target.paths) + len(target.query_paths) for target in cfg.targets)
    return True, (
        f"Valid config: {len(cfg.targets)} target(s), "
        f"{total_paths} path(s), {len(cfg.traffic_profiles)} user-agent profile(s)."
    )


def get_status_map() -> dict[str, dict[str, Any]]:
    statuses = {entry["name"]: entry for entry in get_all_runner_statuses(STATE_FILE)}
    for name in (THREAD_RUNNER_NAME, COMMENT_RUNNER_NAME, LOCUST_RUNNER_NAME, BROWSER_MATRIX_RUNNER_NAME):
        statuses.setdefault(name, {"name": name, "running": False})
    return statuses


def render_header() -> None:
    st.markdown(
        """
<div class="hero-panel">
  <h1>LoadOps Command Center</h1>
  <p>Monitor, edit, generate, and run your authorized traffic tests from one dashboard.</p>
</div>
""",
        unsafe_allow_html=True,
    )


def render_overview() -> None:
    st.markdown("### Overview")
    statuses = get_status_map()
    thread_running = "Running" if statuses[THREAD_RUNNER_NAME]["running"] else "Stopped"
    comment_running = "Running" if statuses[COMMENT_RUNNER_NAME]["running"] else "Stopped"
    locust_running = "Running" if statuses[LOCUST_RUNNER_NAME]["running"] else "Stopped"
    browser_matrix_running = "Running" if statuses[BROWSER_MATRIX_RUNNER_NAME]["running"] else "Stopped"

    targets = 0
    total_paths = 0
    ua_profiles = 0
    auth = "false"
    try:
        cfg = load_test_config(active_config_path())
        targets = len(cfg.targets)
        total_paths = sum(len(target.paths) + len(target.query_paths) for target in cfg.targets)
        ua_profiles = len(cfg.traffic_profiles)
        auth = str(cfg.owner_authorization).lower()
    except Exception:
        pass

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        metric_card("Targets", str(targets), f"Configured pages: {total_paths}")
    with col2:
        metric_card("User Agents", str(ua_profiles), "Weighted profile pool")
    with col3:
        metric_card("Threaded Journey", thread_running, "Page-following runner")
    with col4:
        metric_card("Post Comments", comment_running, "Comment publishing journey")
    with col5:
        metric_card("Locust Engine", locust_running, f"owner_authorization={auth}")
    with col6:
        metric_card("Browser Matrix", browser_matrix_running, "Cross-browser/device sweep")

    config_path = active_config_path()
    if config_path.exists():
        st.caption(f"Config path: {config_path}")
    else:
        st.warning(f"Config file not found. Expected one of: {CONFIG_PRIMARY_PATH} or {CONFIG_EXAMPLE_PATH}")
def render_config_studio() -> None:
    st.markdown("### Config Studio")

    st.markdown("#### Owner Authorization")
    auth_enabled = False
    try:
        cfg = load_test_config(active_config_path())
        auth_enabled = bool(cfg.owner_authorization)
    except Exception:
        auth_enabled = False

    auth_label = "ENABLED" if auth_enabled else "DISABLED"
    auth_message = (
        "Owner authorization is enabled. Protected runners can start."
        if auth_enabled
        else "Owner authorization is disabled. Protected runners will be blocked."
    )
    if auth_enabled:
        st.success(f"Status: {auth_label} - {auth_message}")
    else:
        st.warning(f"Status: {auth_label} - {auth_message}")

    auth_enable_col, auth_disable_col = st.columns(2)
    with auth_enable_col:
        if st.button("Enable Owner Authorization", width='stretch', key='auth_enable_btn', disabled=auth_enabled):
            try:
                set_owner_authorization(True)
                st.session_state["yaml_editor_value"] = load_config_text()
                st.success("owner_authorization enabled.")
                st.rerun()
            except Exception as exc:
                st.error(f"Enable failed: {exc}")
    with auth_disable_col:
        if st.button("Disable Owner Authorization", width='stretch', key='auth_disable_btn', disabled=not auth_enabled):
            try:
                set_owner_authorization(False)
                st.session_state["yaml_editor_value"] = load_config_text()
                st.info("owner_authorization disabled.")
                st.rerun()
            except Exception as exc:
                st.error(f"Disable failed: {exc}")

    import_col, edit_col = st.columns([1, 1.25])
    with import_col:
        st.markdown("#### URL Import Generator")
        st.caption("Provide one or more URL-list files (one URL per line).")
        url_files_raw = st.text_area(
            "URL file paths",
            value="",
            placeholder="/absolute/path/to/urls1.txt\n/absolute/path/to/urls2.txt",
            height=140,
        )
        owner_auth_on_import = st.checkbox(
            "Set owner_authorization=true on generate",
            value=False,
            help="Enable only if you own or have explicit permission to test all targets.",
        )

        if st.button("Generate Config", width='stretch', type="primary"):
            try:
                raw_paths = [line.strip() for line in url_files_raw.splitlines() if line.strip()]
                if not raw_paths:
                    raise ValueError("Provide at least one URL file path.")
                input_paths = [Path(path).expanduser().resolve() for path in raw_paths]
                urls = load_urls_from_files(input_paths)
                targets = build_targets(urls)
                config_data = build_default_config(
                    targets=targets,
                    owner_authorization=owner_auth_on_import,
                )
                CONFIG_PRIMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
                CONFIG_PRIMARY_PATH.write_text(yaml.safe_dump(config_data, sort_keys=False), encoding="utf-8")
                st.session_state["yaml_editor_value"] = load_config_text()
                total_paths = sum(len(target["paths"]) + len(target["query_paths"]) for target in targets)
                st.success(
                    f"Config generated with {len(targets)} target(s), "
                    f"{total_paths} path(s), and {len(config_data['traffic_profiles'])} user agents."
                )
            except Exception as exc:
                st.error(f"Generate failed: {exc}")

    with edit_col:
        st.markdown("#### YAML Editor")
        if st.session_state.pop("yaml_editor_reload_requested", False):
            st.session_state["yaml_editor_value"] = load_config_text()
            st.session_state["yaml_editor_reloaded_notice"] = True

        if "yaml_editor_value" not in st.session_state:
            st.session_state["yaml_editor_value"] = load_config_text()

        st.text_area("Edit config/sites.yaml", key="yaml_editor_value", height=360)
        save_col, reload_col, validate_col = st.columns(3)
        with save_col:
            if st.button("Save YAML", width='stretch'):
                try:
                    save_config_from_text(st.session_state["yaml_editor_value"])
                    st.success("Configuration saved.")
                except Exception as exc:
                    st.error(f"Save failed: {exc}")
        with reload_col:
            if st.button("Reload Disk", width='stretch'):
                st.session_state["yaml_editor_reload_requested"] = True
                st.rerun()
        with validate_col:
            if st.button("Validate", width='stretch'):
                ok, message = validate_current_config()
                if ok:
                    st.success(message)
                else:
                    st.error(f"Invalid config: {message}")

        if st.session_state.pop("yaml_editor_reloaded_notice", False):
            st.info("Reloaded from disk.")

    st.markdown("#### Target Inspector")
    try:
        cfg = load_test_config(active_config_path())
        rows = []
        for target in cfg.targets:
            rows.append(
                {
                    "name": target.name,
                    "base_url": target.base_url,
                    "weight": target.weight,
                    "paths": len(target.paths),
                    "query_paths": len(target.query_paths),
                    "healthcheck": target.healthcheck_path,
                }
            )
        st.dataframe(pd.DataFrame(rows), width='stretch')
    except ConfigError as exc:
        st.warning(f"Unable to render target inspector: {exc}")


def is_streamlit_cloud_runtime() -> bool:
    if os.environ.get("STREAMLIT_SHARING_MODE"):
        return True
    if os.environ.get("STREAMLIT_CLOUD"):
        return True
    if str(PROJECT_ROOT).startswith("/mount/src/"):
        return True
    home = os.environ.get("HOME", "")
    return home.startswith("/home/appuser") or home.startswith("/home/adminuser")


def _clamp_number_input_state(key: str, minimum: float) -> None:
    current = st.session_state.get(key)
    if current is None:
        return
    try:
        if float(current) < float(minimum):
            st.session_state[key] = minimum
    except (TypeError, ValueError):
        st.session_state[key] = minimum


def render_threaded_runner_control(statuses: dict[str, dict[str, Any]]) -> None:
    threaded_defaults = load_threaded_ui_config()

    def _state_or_saved(key: str) -> Any:
        return st.session_state.get(key, threaded_defaults[key])

    st.markdown("#### Threaded Browser Journey")
    st.caption(
        "Each worker opens a random URL, scrolls, waits, clicks internal links, and switches to another seed URL."
    )

    core_col, behavior_col = st.columns(2)
    with core_col:
        cloud_mode = is_streamlit_cloud_runtime()
        thread_cap = 3 if cloud_mode else 200
        raw_threads_value = int(_state_or_saved("thr_threads"))
        clamped_threads_value = max(1, min(thread_cap, raw_threads_value))
        if clamped_threads_value != raw_threads_value:
            st.session_state["thr_threads"] = clamped_threads_value
        threads = st.number_input("Threads", min_value=1, max_value=thread_cap, value=clamped_threads_value, step=1, key="thr_threads")
        if cloud_mode:
            st.warning("Streamlit Cloud detected: threads are capped at 3 to reduce Chromium page crashes.")
        browser_options = ["chromium", "firefox", "webkit"]
        browser_saved = str(_state_or_saved("thr_browser"))
        browser_index = browser_options.index(browser_saved) if browser_saved in browser_options else 0
        browser = st.selectbox(
            "Browser engine",
            options=browser_options,
            index=browser_index,
            key="thr_browser",
        )
        min_clicks = st.number_input(
            "Min internal links per site",
            min_value=1,
            max_value=20,
            value=int(_state_or_saved("thr_min_clicks")),
            step=1,
            key="thr_min_clicks",
        )
        _clamp_number_input_state("thr_max_clicks", int(min_clicks))
        max_clicks_default = int(_state_or_saved("thr_max_clicks"))
        max_clicks = st.number_input(
            "Max internal links per site",
            min_value=int(min_clicks),
            max_value=30,
            value=max(int(min_clicks), max_clicks_default),
            step=1,
            key="thr_max_clicks",
        )
        max_cycles = st.number_input(
            "Max cycles per thread (0 = continuous)",
            min_value=0,
            max_value=100000,
            value=int(_state_or_saved("thr_cycle_limit")),
            step=1,
            key="thr_cycle_limit",
        )
        nav_timeout = st.number_input(
            "Navigation timeout (seconds)",
            min_value=5.0,
            max_value=180.0,
            value=float(_state_or_saved("thr_nav_timeout")),
            step=1.0,
            key="thr_nav_timeout",
        )
        headed = st.checkbox("Headed mode (show browser windows)", value=bool(_state_or_saved("thr_headed")), key="thr_headed")
        metrics_output = st.text_input(
            "Threaded metrics JSON path",
            value=str(_state_or_saved("thr_metrics_output")),
            key="thr_metrics_output",
        )
        metrics_flush_seconds = st.number_input(
            "Metrics flush interval (seconds)",
            min_value=0.5,
            max_value=60.0,
            value=float(_state_or_saved("thr_metrics_flush")),
            step=0.5,
            key="thr_metrics_flush",
        )

    with behavior_col:
        min_page_browse = st.number_input(
            "Min page browse duration (seconds)",
            min_value=1.0,
            max_value=600.0,
            value=float(_state_or_saved("thr_min_page_browse")),
            step=1.0,
            key="thr_min_page_browse",
        )
        _clamp_number_input_state("thr_max_page_browse", float(min_page_browse))
        max_page_browse_default = float(_state_or_saved("thr_max_page_browse"))
        max_page_browse = st.number_input(
            "Max page browse duration (seconds)",
            min_value=float(min_page_browse),
            max_value=900.0,
            value=max(float(min_page_browse), max_page_browse_default),
            step=1.0,
            key="thr_max_page_browse",
        )
        min_scroll = st.number_input(
            "Min scroll time per page (seconds)",
            min_value=0.0,
            max_value=300.0,
            value=float(_state_or_saved("thr_min_scroll")),
            step=0.5,
            key="thr_min_scroll",
        )
        _clamp_number_input_state("thr_max_scroll", float(min_scroll))
        max_scroll_default = float(_state_or_saved("thr_max_scroll"))
        max_scroll = st.number_input(
            "Max scroll time per page (seconds)",
            min_value=float(min_scroll),
            max_value=300.0,
            value=max(float(min_scroll), max_scroll_default),
            step=0.5,
            key="thr_max_scroll",
        )
        min_scroll_pause = st.number_input(
            "Min delay between scroll actions (seconds)",
            min_value=0.0,
            max_value=30.0,
            value=float(_state_or_saved("thr_min_scroll_pause")),
            step=0.1,
            key="thr_min_scroll_pause",
        )
        _clamp_number_input_state("thr_max_scroll_pause", float(min_scroll_pause))
        max_scroll_pause_default = float(_state_or_saved("thr_max_scroll_pause"))
        max_scroll_pause = st.number_input(
            "Max delay between scroll actions (seconds)",
            min_value=float(min_scroll_pause),
            max_value=30.0,
            value=max(float(min_scroll_pause), max_scroll_pause_default),
            step=0.1,
            key="thr_max_scroll_pause",
        )
        min_post_scroll_delay = st.number_input(
            "Min delay between scroll and link click (seconds)",
            min_value=0.0,
            max_value=120.0,
            value=float(_state_or_saved("thr_min_post_scroll_delay")),
            step=0.1,
            key="thr_min_post_scroll_delay",
        )
        _clamp_number_input_state("thr_max_post_scroll_delay", float(min_post_scroll_delay))
        max_post_scroll_delay_default = float(_state_or_saved("thr_max_post_scroll_delay"))
        max_post_scroll_delay = st.number_input(
            "Max delay between scroll and link click (seconds)",
            min_value=float(min_post_scroll_delay),
            max_value=120.0,
            value=max(float(min_post_scroll_delay), max_post_scroll_delay_default),
            step=0.1,
            key="thr_max_post_scroll_delay",
        )
        min_site_browse = st.number_input(
            "Min total time per site cycle (seconds, 0=disabled)",
            min_value=0.0,
            max_value=3600.0,
            value=float(_state_or_saved("thr_min_site_browse")),
            step=1.0,
            key="thr_min_site_browse",
        )
        _clamp_number_input_state("thr_max_site_browse", float(min_site_browse))
        max_site_browse_default = float(_state_or_saved("thr_max_site_browse"))
        max_site_browse = st.number_input(
            "Max total time per site cycle (seconds, 0=disabled)",
            min_value=float(min_site_browse),
            max_value=3600.0,
            value=max(float(min_site_browse), max_site_browse_default),
            step=1.0,
            key="thr_max_site_browse",
        )
        scroll_step_px = st.number_input(
            "Scroll step size (px)",
            min_value=50,
            max_value=5000,
            value=int(_state_or_saved("thr_scroll_step")),
            step=50,
            key="thr_scroll_step",
        )
        disable_scroll = st.checkbox("Disable scrolling behavior", value=bool(_state_or_saved("thr_disable_scroll")), key="thr_disable_scroll")

    thread_cmd = [
        sys.executable,
        "scripts/threaded_browser_journey.py",
        "--config",
        str(writable_config_path()),
        "--threads",
        str(int(threads)),
        "--browser",
        browser,
        "--min-clicks",
        str(int(min_clicks)),
        "--max-clicks",
        str(int(max_clicks)),
        "--min-page-browse-seconds",
        str(float(min_page_browse)),
        "--max-page-browse-seconds",
        str(float(max_page_browse)),
        "--min-scroll-seconds",
        str(float(min_scroll)),
        "--max-scroll-seconds",
        str(float(max_scroll)),
        "--min-scroll-pause-seconds",
        str(float(min_scroll_pause)),
        "--max-scroll-pause-seconds",
        str(float(max_scroll_pause)),
        "--min-post-scroll-click-delay-seconds",
        str(float(min_post_scroll_delay)),
        "--max-post-scroll-click-delay-seconds",
        str(float(max_post_scroll_delay)),
        "--min-site-browse-seconds",
        str(float(min_site_browse)),
        "--max-site-browse-seconds",
        str(float(max_site_browse)),
        "--scroll-step-px",
        str(int(scroll_step_px)),
        "--navigation-timeout-seconds",
        str(float(nav_timeout)),
        "--max-cycles-per-thread",
        str(int(max_cycles)),
        "--metrics-output",
        metrics_output,
        "--metrics-flush-seconds",
        str(float(metrics_flush_seconds)),
    ]
    if disable_scroll:
        thread_cmd.append("--disable-scroll")
    if headed:
        thread_cmd.append("--headed")

    st.code(" ".join(thread_cmd), language="bash")
    save_col, saved_path_col = st.columns([1, 2])
    with save_col:
        if st.button("Save Threaded Config", width='stretch', key="thr_save_config"):
            try:
                save_threaded_ui_config_from_session()
                st.success("Threaded config saved.")
            except Exception as exc:
                st.error(f"Save failed: {exc}")
    with saved_path_col:
        st.caption(f"Saved config file: {THREADED_UI_CONFIG_FILE}")

    thread_start_col, thread_stop_col, thread_force_col = st.columns(3)
    with thread_start_col:
        if st.button("Start Threaded Runner", width='stretch', key="thr_start"):
            try:
                status = start_runner(
                    state_file=STATE_FILE,
                    runner_name=THREAD_RUNNER_NAME,
                    cmd=thread_cmd,
                    cwd=PROJECT_ROOT,
                    log_file=THREAD_LOG_FILE,
                    env=os.environ.copy(),
                )
                st.success(f"Started {status['name']} with PID {status['pid']}.")
            except Exception as exc:
                st.error(f"Start failed: {exc}")
    with thread_stop_col:
        if st.button("Stop Threaded Runner", width='stretch', key="thr_stop"):
            status = stop_runner(state_file=STATE_FILE, runner_name=THREAD_RUNNER_NAME)
            if status.get("running"):
                st.warning("Stop signal sent.")
            else:
                st.info("Threaded runner stopped.")
    with thread_force_col:
        if st.button("Force Kill Threaded", width='stretch', key="thr_force"):
            stop_runner(state_file=STATE_FILE, runner_name=THREAD_RUNNER_NAME, force=True)
            st.warning("Force kill signal sent.")

    thread_state = "running" if statuses[THREAD_RUNNER_NAME]["running"] else "stopped"
    st.caption(f"Current status: {thread_state}")


def render_comment_runner_control(statuses: dict[str, dict[str, Any]]) -> None:
    comment_defaults = load_comment_ui_config()

    def _state_or_saved(key: str) -> Any:
        return st.session_state.get(key, comment_defaults[key])

    st.markdown("#### Post Comments Journey")
    st.caption("Each thread opens a configured URL, waits, posts a comment, publishes, then moves to the next URL.")

    core_col, comment_col = st.columns(2)
    with core_col:
        threads = st.number_input("Threads", min_value=1, max_value=200, value=int(_state_or_saved("com_threads")), step=1, key="com_threads")
        browser_options = ["chromium", "firefox", "webkit"]
        browser_saved = str(_state_or_saved("com_browser"))
        browser_index = browser_options.index(browser_saved) if browser_saved in browser_options else 0
        browser = st.selectbox(
            "Browser engine",
            options=browser_options,
            index=browser_index,
            key="com_browser",
        )
        max_cycles = st.number_input(
            "Max cycles per thread (0 = continuous)",
            min_value=0,
            max_value=100000,
            value=int(_state_or_saved("com_cycle_limit")),
            step=1,
            key="com_cycle_limit",
        )
        min_wait_seconds = st.number_input(
            "Min wait before posting comment (seconds)",
            min_value=0.0,
            max_value=600.0,
            value=float(_state_or_saved("com_min_wait_seconds")),
            step=0.5,
            key="com_min_wait_seconds",
        )
        _clamp_number_input_state("com_max_wait_seconds", float(min_wait_seconds))
        max_wait_default = float(_state_or_saved("com_max_wait_seconds"))
        max_wait_seconds = st.number_input(
            "Max wait before posting comment (seconds)",
            min_value=float(min_wait_seconds),
            max_value=1200.0,
            value=max(float(min_wait_seconds), max_wait_default),
            step=0.5,
            key="com_max_wait_seconds",
        )
        nav_timeout = st.number_input(
            "Navigation timeout (seconds)",
            min_value=5.0,
            max_value=180.0,
            value=float(_state_or_saved("com_nav_timeout")),
            step=1.0,
            key="com_nav_timeout",
        )
        headed = st.checkbox("Headed mode (show browser windows)", value=bool(_state_or_saved("com_headed")), key="com_headed")
        metrics_output = st.text_input(
            "Comment runner metrics JSON path",
            value=str(_state_or_saved("com_metrics_output")),
            key="com_metrics_output",
        )
        metrics_flush_seconds = st.number_input(
            "Metrics flush interval (seconds)",
            min_value=0.5,
            max_value=60.0,
            value=float(_state_or_saved("com_metrics_flush")),
            step=0.5,
            key="com_metrics_flush",
        )

    with comment_col:
        profile_mode_options = ["anonymous", "name", "mixed"]
        profile_mode_labels = {
            "anonymous": "Anonymous only",
            "name": "Name only",
            "mixed": "Mixed random (Anonymous/Name)",
        }
        profile_mode_saved = str(_state_or_saved("com_comment_profile_mode"))
        profile_mode_index = profile_mode_options.index(profile_mode_saved) if profile_mode_saved in profile_mode_options else 0
        comment_profile_mode = st.selectbox(
            "Comment profile type",
            options=profile_mode_options,
            index=profile_mode_index,
            format_func=lambda mode: profile_mode_labels.get(mode, mode),
            key="com_comment_profile_mode",
        )
        comments_file = st.text_input(
            "Comments file path (.txt/.csv/.json)",
            value=str(_state_or_saved("com_comments_file")),
            key="com_comments_file",
            placeholder="/absolute/path/to/comments.txt",
        )
        names_file = st.text_input(
            "Names file path (.txt/.rtf/.csv/.json)",
            value=str(_state_or_saved("com_comment_names_file")),
            key="com_comment_names_file",
            disabled=comment_profile_mode == "anonymous",
            placeholder="/absolute/path/to/names.rtf",
        )
        st.caption("Comment posting attempt rate is fixed at 100% for this journey.")

    comment_config_error: str | None = None
    if not str(comments_file).strip():
        comment_config_error = "Comments file is required for Post Comments runner."
    elif comment_profile_mode in {"name", "mixed"} and not str(names_file).strip():
        comment_config_error = "Names file is required for Name or Mixed comment profile mode."

    if comment_config_error:
        st.warning(comment_config_error)

    comment_cmd = [
        sys.executable,
        "scripts/threaded_browser_journey.py",
        "--config",
        str(writable_config_path()),
        "--threads",
        str(int(threads)),
        "--browser",
        browser,
        "--navigation-timeout-seconds",
        str(float(nav_timeout)),
        "--max-cycles-per-thread",
        str(int(max_cycles)),
        "--metrics-output",
        metrics_output,
        "--metrics-flush-seconds",
        str(float(metrics_flush_seconds)),
        "--enable-comments",
        "--comment-simple-mode",
        "--comment-profile-mode",
        comment_profile_mode,
        "--comments-file",
        str(comments_file).strip(),
        "--comment-attempt-rate",
        "1.0",
        "--comment-min-wait-seconds",
        str(float(min_wait_seconds)),
        "--comment-max-wait-seconds",
        str(float(max_wait_seconds)),
    ]
    if comment_profile_mode in {"name", "mixed"}:
        comment_cmd.extend(["--comment-names-file", str(names_file).strip()])
    if headed:
        comment_cmd.append("--headed")

    st.code(" ".join(comment_cmd), language="bash")
    save_col, saved_path_col = st.columns([1, 2])
    with save_col:
        if st.button("Save Comment Config", width='stretch', key="com_save_config"):
            try:
                save_comment_ui_config_from_session()
                st.success("Comment runner config saved.")
            except Exception as exc:
                st.error(f"Save failed: {exc}")
    with saved_path_col:
        st.caption(f"Saved config file: {COMMENT_UI_CONFIG_FILE}")

    start_col, stop_col, force_col = st.columns(3)
    with start_col:
        if st.button("Start Comment Runner", width='stretch', key="com_start", disabled=bool(comment_config_error)):
            try:
                status = start_runner(
                    state_file=STATE_FILE,
                    runner_name=COMMENT_RUNNER_NAME,
                    cmd=comment_cmd,
                    cwd=PROJECT_ROOT,
                    log_file=COMMENT_LOG_FILE,
                    env=os.environ.copy(),
                )
                st.success(f"Started {status['name']} with PID {status['pid']}.")
            except Exception as exc:
                st.error(f"Start failed: {exc}")
    with stop_col:
        if st.button("Stop Comment Runner", width='stretch', key="com_stop"):
            status = stop_runner(state_file=STATE_FILE, runner_name=COMMENT_RUNNER_NAME)
            if status.get("running"):
                st.warning("Stop signal sent.")
            else:
                st.info("Comment runner stopped.")
    with force_col:
        if st.button("Force Kill Comments", width='stretch', key="com_force"):
            stop_runner(state_file=STATE_FILE, runner_name=COMMENT_RUNNER_NAME, force=True)
            st.warning("Force kill signal sent.")

    comment_state = "running" if statuses[COMMENT_RUNNER_NAME]["running"] else "stopped"
    st.caption(f"Current status: {comment_state}")


def render_locust_runner_control(statuses: dict[str, dict[str, Any]]) -> None:
    st.markdown("#### Locust Headless Load")
    users = st.number_input("Concurrent users", min_value=1, max_value=200000, value=60, step=1, key="loc_users")
    spawn_rate = st.number_input("Spawn rate", min_value=1.0, max_value=5000.0, value=4.0, step=1.0, key="loc_spawn")
    run_time = st.text_input("Run time", value="30m", help="Examples: 30s, 10m, 1h", key="loc_runtime")
    results_dir = st.text_input("Results directory", value=str(RESULTS_DIR), key="loc_results_dir")
    web_ui = st.checkbox("Enable Locust Web UI", value=False, key="loc_webui")

    if users >= 200 or spawn_rate >= 10:
        st.warning("High-load profile selected. Expect significant 429/503 unless the target is provisioned for this level.")

    locust_cmd = [
        sys.executable,
        "scripts/run_locust.py",
        "--config",
        str(writable_config_path()),
        "--users",
        str(int(users)),
        "--spawn-rate",
        str(float(spawn_rate)),
        "--run-time",
        run_time,
        "--results-dir",
        results_dir,
    ]
    if web_ui:
        locust_cmd.append("--web-ui")

    st.code(" ".join(locust_cmd), language="bash")
    locust_start_col, locust_stop_col, locust_force_col = st.columns(3)
    with locust_start_col:
        if st.button("Start Locust Runner", width='stretch', key="loc_start"):
            try:
                status = start_runner(
                    state_file=STATE_FILE,
                    runner_name=LOCUST_RUNNER_NAME,
                    cmd=locust_cmd,
                    cwd=PROJECT_ROOT,
                    log_file=LOCUST_LOG_FILE,
                    env=os.environ.copy(),
                )
                st.success(f"Started {status['name']} with PID {status['pid']}.")
            except Exception as exc:
                st.error(f"Start failed: {exc}")
    with locust_stop_col:
        if st.button("Stop Locust Runner", width='stretch', key="loc_stop"):
            status = stop_runner(state_file=STATE_FILE, runner_name=LOCUST_RUNNER_NAME)
            if status.get("running"):
                st.warning("Stop signal sent.")
            else:
                st.info("Locust runner stopped.")
    with locust_force_col:
        if st.button("Force Kill Locust", width='stretch', key="loc_force"):
            stop_runner(state_file=STATE_FILE, runner_name=LOCUST_RUNNER_NAME, force=True)
            st.warning("Force kill signal sent.")

    locust_state = "running" if statuses[LOCUST_RUNNER_NAME]["running"] else "stopped"
    st.caption(f"Current status: {locust_state}")


def render_matrix_runner_control(statuses: dict[str, dict[str, Any]]) -> None:
    st.markdown("#### Browser & Device Matrix")
    st.caption("Runs compatibility checks across selected browsers, devices, and target paths.")
    browsers = st.multiselect(
        "Browsers",
        options=["chromium", "firefox", "webkit"],
        default=["chromium", "firefox", "webkit"],
        key="mat_browsers",
    )
    devices = st.multiselect(
        "Devices",
        options=list(DEFAULT_BROWSER_MATRIX_DEVICES),
        default=list(DEFAULT_BROWSER_MATRIX_DEVICES),
        key="mat_devices",
    )
    paths_per_target = st.number_input(
        "Paths per target",
        min_value=1,
        max_value=1000,
        value=2,
        step=1,
        key="mat_paths_per_target",
    )
    output_json = st.text_input("Output JSON path", value=str(BROWSER_MATRIX_OUTPUT_JSON), key="mat_output_json")

    can_run_matrix = True
    if not browsers:
        st.warning("Select at least one browser.")
        can_run_matrix = False
    if not devices:
        st.warning("Select at least one device.")
        can_run_matrix = False

    matrix_cmd = [
        sys.executable,
        "scripts/browser_matrix.py",
        "--config",
        str(writable_config_path()),
        "--paths-per-target",
        str(int(paths_per_target)),
        "--output-json",
        output_json,
    ]
    if browsers:
        matrix_cmd.extend(["--browsers", *browsers])
    if devices:
        matrix_cmd.extend(["--devices", *devices])

    st.code(" ".join(matrix_cmd), language="bash")

    try:
        cfg = load_test_config(active_config_path())
        total_urls = sum(min(int(paths_per_target), len(dict.fromkeys(t.paths + t.query_paths))) for t in cfg.targets)
        estimated_checks = total_urls * max(1, len(browsers)) * max(1, len(devices))
        st.caption(
            f"Estimated checks: {estimated_checks} "
            f"({total_urls} URLs x {len(browsers)} browser(s) x {len(devices)} device(s))"
        )
    except Exception:
        pass

    matrix_start_col, matrix_stop_col, matrix_force_col = st.columns(3)
    with matrix_start_col:
        if st.button(
            "Start Matrix Run",
            width='stretch',
            key="mat_start",
            disabled=not can_run_matrix,
        ):
            try:
                status = start_runner(
                    state_file=STATE_FILE,
                    runner_name=BROWSER_MATRIX_RUNNER_NAME,
                    cmd=matrix_cmd,
                    cwd=PROJECT_ROOT,
                    log_file=BROWSER_MATRIX_LOG_FILE,
                    env=os.environ.copy(),
                )
                st.success(f"Started {status['name']} with PID {status['pid']}.")
            except Exception as exc:
                st.error(f"Start failed: {exc}")
    with matrix_stop_col:
        if st.button("Stop Matrix Run", width='stretch', key="mat_stop"):
            status = stop_runner(state_file=STATE_FILE, runner_name=BROWSER_MATRIX_RUNNER_NAME)
            if status.get("running"):
                st.warning("Stop signal sent.")
            else:
                st.info("Matrix runner stopped.")
    with matrix_force_col:
        if st.button("Force Kill Matrix", width='stretch', key="mat_force"):
            stop_runner(state_file=STATE_FILE, runner_name=BROWSER_MATRIX_RUNNER_NAME, force=True)
            st.warning("Force kill signal sent.")

    matrix_state = "running" if statuses[BROWSER_MATRIX_RUNNER_NAME]["running"] else "stopped"
    st.caption(f"Current status: {matrix_state}")


def render_runner_control() -> None:
    st.markdown("### Runner Control")
    statuses = get_status_map()
    tab_threaded, tab_comments, tab_locust, tab_matrix = st.tabs(
        ["Threaded Journey", "Post Comments", "Locust Load", "Browser Matrix"]
    )
    with tab_threaded:
        render_threaded_runner_control(statuses)
    with tab_comments:
        render_comment_runner_control(statuses)
    with tab_locust:
        render_locust_runner_control(statuses)
    with tab_matrix:
        render_matrix_runner_control(statuses)


def _runner_status_frame(statuses: dict[str, dict[str, Any]], runner_name: str) -> pd.DataFrame:
    row = statuses.get(runner_name, {"name": runner_name, "running": False})
    return pd.DataFrame(
        [
            {
                "runner": runner_name,
                "running": row.get("running", False),
                "pid": row.get("pid"),
                "started_at_utc": row.get("started_at"),
                "log_file": row.get("log_file"),
            }
        ]
    )


def render_auto_refresh_dropdown(key: str) -> float | None:
    options = {
        "Off": 0.0,
        "1S": 1.0,
        "3S": 3.0,
        "5S": 5.0,
    }
    refresh_col, _spacer_col = st.columns([1, 4])
    with refresh_col:
        selected = st.selectbox(
            "Auto refresh",
            options=list(options.keys()),
            index=0,
            key=key,
        )
    seconds = float(options[selected])
    return seconds if seconds > 0 else None


def render_threaded_workspace() -> None:
    st.markdown("### Threaded Journey")
    statuses = get_status_map()
    render_threaded_runner_control(statuses)

    st.markdown("#### Live Monitoring")
    refresh_seconds = render_auto_refresh_dropdown("thr_auto_refresh_interval")
    st.dataframe(_runner_status_frame(get_status_map(), THREAD_RUNNER_NAME), width='stretch')
    render_threaded_insights()

    st.markdown("#### Threaded Runner Log")
    status = get_status_map()[THREAD_RUNNER_NAME]
    log_path = Path(status.get("log_file", THREAD_LOG_FILE))
    st.code(read_log_tail(log_path, lines=120) or "No log output yet.", language="text")

    if refresh_seconds:
        time.sleep(refresh_seconds)
        st.rerun()


def render_comment_workspace() -> None:
    st.markdown("### Post Comments")
    statuses = get_status_map()
    render_comment_runner_control(statuses)

    st.markdown("#### Live Monitoring")
    refresh_seconds = render_auto_refresh_dropdown("com_auto_refresh_interval")
    st.dataframe(_runner_status_frame(get_status_map(), COMMENT_RUNNER_NAME), width='stretch')
    render_comment_insights()

    st.markdown("#### Comment Runner Log")
    status = get_status_map()[COMMENT_RUNNER_NAME]
    log_path = Path(status.get("log_file", COMMENT_LOG_FILE))
    st.code(read_log_tail(log_path, lines=120) or "No log output yet.", language="text")

    if refresh_seconds:
        time.sleep(refresh_seconds)
        st.rerun()



def render_locust_workspace() -> None:
    st.markdown("### Locust Load")
    statuses = get_status_map()
    render_locust_runner_control(statuses)

    st.markdown("#### Live Monitoring")
    refresh_seconds = render_auto_refresh_dropdown("loc_auto_refresh_interval")
    st.dataframe(_runner_status_frame(get_status_map(), LOCUST_RUNNER_NAME), width='stretch')
    st.markdown("#### Locust Runner Log")
    status = get_status_map()[LOCUST_RUNNER_NAME]
    log_path = Path(status.get("log_file", LOCUST_LOG_FILE))
    st.code(read_log_tail(log_path, lines=120) or "No log output yet.", language="text")
    render_locust_insights()

    if refresh_seconds:
        time.sleep(refresh_seconds)
        st.rerun()


def render_matrix_workspace() -> None:
    st.markdown("### Browser Matrix")
    statuses = get_status_map()
    render_matrix_runner_control(statuses)

    st.markdown("#### Live Monitoring")
    refresh_seconds = render_auto_refresh_dropdown("mat_auto_refresh_interval")
    st.dataframe(_runner_status_frame(get_status_map(), BROWSER_MATRIX_RUNNER_NAME), width='stretch')
    st.markdown("#### Browser Matrix Log")
    status = get_status_map()[BROWSER_MATRIX_RUNNER_NAME]
    log_path = Path(status.get("log_file", BROWSER_MATRIX_LOG_FILE))
    st.code(read_log_tail(log_path, lines=120) or "No log output yet.", language="text")
    render_browser_matrix_insights()

    if refresh_seconds:
        time.sleep(refresh_seconds)
        st.rerun()


def find_latest_stats_csv() -> Path | None:
    files = sorted(RESULTS_DIR.glob("loadtest-*_stats.csv"), key=lambda x: x.stat().st_mtime, reverse=True)
    return files[0] if files else None



def render_journey_insights(
    section_title: str,
    output_session_key: str,
    default_output_path: Path,
    include_comment_metrics: bool = False,
) -> None:
    st.markdown(section_title)
    output_path = Path(st.session_state.get(output_session_key, str(default_output_path)))
    if not output_path.exists():
        st.info("No runner metrics JSON output found yet.")
        return

    st.caption(f"Latest metrics file: {output_path.name}")
    try:
        payload = json.loads(output_path.read_text(encoding="utf-8"))
    except Exception as exc:
        st.error(f"Unable to parse runner metrics JSON: {exc}")
        return

    summary = payload.get("summary", {})
    runner = payload.get("runner", {})
    threads = payload.get("threads", [])

    if not isinstance(summary, dict) or not isinstance(runner, dict) or not isinstance(threads, list):
        st.warning("Unexpected runner metrics schema.")
        return

    def _n(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    cycles_started = int(_n(summary.get("cycles_started")))
    cycles_completed = int(_n(summary.get("cycles_completed")))
    pages_opened = int(_n(summary.get("pages_opened")))
    active_threads = int(_n(summary.get("threads_alive")))
    nav_success_pct = _n(summary.get("navigation_success_rate_pct"))
    avg_nav_ms = _n(summary.get("navigation_avg_ms"))
    click_completion_pct = _n(summary.get("click_completion_rate_pct"))
    scroll_actions = int(_n(summary.get("scroll_actions")))
    unique_urls = int(_n(summary.get("unique_urls_seen")))
    http_error_pages = int(_n(summary.get("http_error_pages")))
    uptime_seconds = _n(summary.get("uptime_seconds"))

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Active Threads", active_threads)
    c2.metric("Cycles", f"{cycles_completed}/{cycles_started}")
    c3.metric("Pages Opened", pages_opened)
    c4.metric("Nav Success %", round(nav_success_pct, 2))
    c5.metric("Avg Nav (ms)", round(avg_nav_ms, 2))

    c6, c7, c8, c9, c10 = st.columns(5)
    c6.metric("Click Completion %", round(click_completion_pct, 2))
    c7.metric("Scroll Actions", scroll_actions)
    c8.metric("Unique URLs", unique_urls)
    c9.metric("HTTP Error Pages", http_error_pages)
    c10.metric("Uptime (s)", round(uptime_seconds, 1))

    pages_per_minute = _n(summary.get("pages_per_minute"))
    cycles_per_minute = _n(summary.get("cycles_per_minute"))
    clicks_per_minute = _n(summary.get("clicks_per_minute"))
    st.caption(
        f"Throughput | pages/min={pages_per_minute:.2f}, cycles/min={cycles_per_minute:.2f}, clicks/min={clicks_per_minute:.2f}"
    )

    if include_comment_metrics:
        comment_attempts = int(_n(summary.get("comment_attempts")))
        comment_forms_detected = int(_n(summary.get("comment_forms_detected")))
        comment_publish_attempts = int(_n(summary.get("comment_publish_attempts")))
        comment_publish_successes = int(_n(summary.get("comment_publish_successes")))
        comment_publish_failures = int(_n(summary.get("comment_publish_failures")))
        comment_captcha_detected = int(_n(summary.get("comment_captcha_detected")))
        comment_success_rate = _n(summary.get("comment_publish_success_rate_pct"))

        c11, c12, c13, c14, c15 = st.columns(5)
        c11.metric("Comment Attempts", comment_attempts)
        c12.metric("Forms Detected", comment_forms_detected)
        c13.metric("Publish Success", comment_publish_successes)
        c14.metric("Publish Failures", comment_publish_failures)
        c15.metric("Comment Success %", round(comment_success_rate, 2))
        st.caption(
            f"Comment detail | publish attempts={comment_publish_attempts}, captcha detected={comment_captcha_detected}"
        )

    status_counts = summary.get("status_counts", {})
    if isinstance(status_counts, dict):
        status_df = pd.DataFrame(
            [{"status": key, "count": int(value)} for key, value in status_counts.items() if int(value) > 0]
        )
        if not status_df.empty:
            fig = px.bar(
                status_df,
                x="status",
                y="count",
                title="Threaded Navigation Status Distribution",
                color="count",
                color_continuous_scale=["#2a9d8f", "#f4a261", "#e76f51"],
            )
            fig.update_layout(xaxis_title="Status Bucket", yaxis_title="Count")
            st.plotly_chart(fig, width='stretch')

    error_counts = summary.get("error_counts", {})
    if isinstance(error_counts, dict) and error_counts:
        error_df = pd.DataFrame(
            [{"error": key, "count": int(value)} for key, value in error_counts.items() if int(value) > 0]
        )
        if not error_df.empty:
            fig = px.bar(
                error_df,
                x="error",
                y="count",
                title="Threaded Error Distribution",
                color="count",
                color_continuous_scale=["#2a9d8f", "#f4a261", "#e76f51"],
            )
            fig.update_layout(xaxis_title="Error Kind", yaxis_title="Count")
            st.plotly_chart(fig, width='stretch')

    top_hits = summary.get("top_visited_urls", [])
    top_failed = summary.get("top_failed_urls", [])
    top_col1, top_col2 = st.columns(2)
    with top_col1:
        st.markdown("##### Top Visited URLs")
        if top_hits:
            st.dataframe(pd.DataFrame(top_hits), width='stretch', height=220)
        else:
            st.info("No visited URL data yet.")
    with top_col2:
        st.markdown("##### Top Failed URLs")
        if top_failed:
            st.dataframe(pd.DataFrame(top_failed), width='stretch', height=220)
        else:
            st.info("No failed URL data yet.")

    if not threads:
        st.info("No per-thread rows in metrics output yet.")
        return

    rows = []
    for row in threads:
        nav_attempts = int(_n(row.get("navigation_attempts")))
        nav_successes = int(_n(row.get("navigation_successes")))
        avg_thread_nav_ms = (
            _n(row.get("navigation_elapsed_total_ms")) / nav_attempts if nav_attempts else 0.0
        )
        status = row.get("status_counts") if isinstance(row.get("status_counts"), dict) else {}

        item = {
            "worker_id": row.get("worker_id"),
            "state": row.get("state"),
            "alive": bool(row.get("alive")),
            "cycles_started": int(_n(row.get("cycles_started"))),
            "cycles_completed": int(_n(row.get("cycles_completed"))),
            "cycles_failed": int(_n(row.get("cycles_failed"))),
            "pages_opened": int(_n(row.get("pages_opened"))),
            "clicks_completed": int(_n(row.get("clicks_completed"))),
            "nav_attempts": nav_attempts,
            "nav_success_pct": round((nav_successes / nav_attempts * 100.0), 2) if nav_attempts else 0.0,
            "avg_nav_ms": round(avg_thread_nav_ms, 2),
            "timeouts": int(_n(row.get("timeouts"))),
            "playwright_errors": int(_n(row.get("playwright_errors"))),
            "unexpected_errors": int(_n(row.get("unexpected_errors"))),
            "2xx": int(_n(status.get("2xx"))),
            "3xx": int(_n(status.get("3xx"))),
            "4xx": int(_n(status.get("4xx"))),
            "5xx": int(_n(status.get("5xx"))),
            "last_status": row.get("last_status_code"),
            "last_url": row.get("last_url"),
            "last_error": row.get("last_error"),
            "last_activity_utc": row.get("last_activity_utc"),
        }
        if include_comment_metrics:
            item.update(
                {
                    "comment_attempts": int(_n(row.get("comment_attempts"))),
                    "comment_forms": int(_n(row.get("comment_forms_detected"))),
                    "comment_publish_ok": int(_n(row.get("comment_publish_successes"))),
                    "comment_publish_fail": int(_n(row.get("comment_publish_failures"))),
                    "comment_captcha": int(_n(row.get("comment_captcha_detected"))),
                }
            )

        rows.append(item)

    st.markdown("##### Per-Thread Metrics")
    st.dataframe(pd.DataFrame(rows), width='stretch', height=340)


def render_threaded_insights() -> None:
    render_journey_insights(
        section_title="#### Threaded Runner Metrics",
        output_session_key="thr_metrics_output",
        default_output_path=THREAD_METRICS_FILE,
        include_comment_metrics=False,
    )


def render_comment_insights() -> None:
    render_journey_insights(
        section_title="#### Comment Runner Metrics",
        output_session_key="com_metrics_output",
        default_output_path=COMMENT_METRICS_FILE,
        include_comment_metrics=True,
    )



def render_locust_insights() -> None:
    st.markdown("#### Locust Metrics")
    latest = find_latest_stats_csv()
    if not latest:
        st.info("No Locust stats file found yet.")
        return

    st.caption(f"Latest stats file: {latest.name}")
    try:
        df = pd.read_csv(latest)
    except Exception as exc:
        st.error(f"Unable to parse {latest}: {exc}")
        return

    if "Name" not in df.columns:
        st.warning("Unexpected CSV schema. Missing 'Name' column.")
        return

    agg_mask = df["Name"].astype(str).str.lower().eq("aggregated")
    aggregate = df[agg_mask]
    endpoints = df[~agg_mask].copy()

    request_col = "Request Count" if "Request Count" in df.columns else None
    failure_col = "Failure Count" if "Failure Count" in df.columns else None
    avg_col = "Average Response Time" if "Average Response Time" in df.columns else None
    p95_col = "95%" if "95%" in df.columns else None

    if not aggregate.empty and all(col is not None for col in [request_col, failure_col, avg_col]):
        row = aggregate.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Requests", int(row[request_col]))
        c2.metric("Failures", int(row[failure_col]))
        c3.metric("Avg RT (ms)", round(float(row[avg_col]), 2))
        c4.metric("P95 (ms)", round(float(row[p95_col]), 2) if p95_col else "n/a")

    if endpoints.empty:
        st.info("No endpoint-level rows found yet.")
        return

    display_cols = [col for col in ["Type", "Name", "Request Count", "Failure Count", "Average Response Time", "95%"] if col in endpoints.columns]
    st.dataframe(endpoints[display_cols], width='stretch', height=280)

    if all(col in endpoints.columns for col in ["Name", "Average Response Time", "Failure Count"]):
        top = endpoints.sort_values("Request Count", ascending=False).head(20) if "Request Count" in endpoints.columns else endpoints.head(20)
        fig = px.bar(
            top,
            x="Name",
            y="Average Response Time",
            color="Failure Count",
            title="Endpoint Response Time (Top Traffic Endpoints)",
            color_continuous_scale=["#2a9d8f", "#f4a261", "#e76f51"],
        )
        fig.update_layout(xaxis_title="Endpoint", yaxis_title="Avg Response Time (ms)")
        st.plotly_chart(fig, width='stretch')


def render_browser_matrix_insights() -> None:
    st.markdown("#### Browser Matrix Metrics")
    output_path = Path(st.session_state.get("mat_output_json", str(BROWSER_MATRIX_OUTPUT_JSON)))
    if not output_path.exists():
        st.info("No browser matrix JSON output found yet.")
        return

    st.caption(f"Latest matrix file: {output_path.name}")
    try:
        rows = json.loads(output_path.read_text(encoding="utf-8"))
        df = pd.DataFrame(rows)
    except Exception as exc:
        st.error(f"Unable to parse browser matrix JSON: {exc}")
        return

    if df.empty:
        st.info("Browser matrix output is empty.")
        return
    if "ok" not in df.columns:
        st.warning("Unexpected browser matrix schema. Missing 'ok' column.")
        return

    total = int(len(df))
    passed = int(df["ok"].sum())
    failed = total - passed
    c1, c2, c3 = st.columns(3)
    c1.metric("Total checks", total)
    c2.metric("Passed", passed)
    c3.metric("Failed", failed)

    cols = [col for col in ["target", "browser", "device", "status", "elapsed_ms", "ok", "error"] if col in df.columns]
    st.dataframe(df[cols], width='stretch', height=260)

    if all(col in df.columns for col in ["browser", "ok"]):
        by_browser = (
            df.groupby("browser", as_index=False)["ok"]
            .mean()
            .rename(columns={"ok": "pass_rate"})
            .sort_values("pass_rate", ascending=False)
        )
        by_browser["pass_rate_pct"] = (by_browser["pass_rate"] * 100).round(2)
        fig = px.bar(
            by_browser,
            x="browser",
            y="pass_rate_pct",
            title="Pass Rate by Browser",
            color="pass_rate_pct",
            color_continuous_scale=["#e76f51", "#f4a261", "#2a9d8f"],
        )
        fig.update_layout(yaxis_title="Pass Rate (%)", xaxis_title="Browser")
        st.plotly_chart(fig, width='stretch')


def render_live_monitor() -> None:
    st.markdown("### Live Monitor")
    auto_refresh = st.checkbox("Auto refresh every 5 seconds", value=False)

    statuses = get_status_map()
    rows = []
    for name in [THREAD_RUNNER_NAME, COMMENT_RUNNER_NAME, LOCUST_RUNNER_NAME, BROWSER_MATRIX_RUNNER_NAME]:
        row = statuses[name]
        rows.append(
            {
                "runner": name,
                "running": row.get("running", False),
                "pid": row.get("pid"),
                "started_at_utc": row.get("started_at"),
                "log_file": row.get("log_file"),
            }
        )
    st.dataframe(pd.DataFrame(rows), width='stretch')

    render_threaded_insights()

    logs_col1, logs_col2, logs_col3, logs_col4 = st.columns(4)
    with logs_col1:
        st.markdown("#### Threaded Runner Log")
        status = statuses[THREAD_RUNNER_NAME]
        log_path = Path(status.get("log_file", THREAD_LOG_FILE))
        st.code(read_log_tail(log_path, lines=70) or "No log output yet.", language="text")
    with logs_col2:
        st.markdown("#### Comment Runner Log")
        status = statuses[COMMENT_RUNNER_NAME]
        log_path = Path(status.get("log_file", COMMENT_LOG_FILE))
        st.code(read_log_tail(log_path, lines=70) or "No log output yet.", language="text")
    with logs_col3:
        st.markdown("#### Locust Runner Log")
        status = statuses[LOCUST_RUNNER_NAME]
        log_path = Path(status.get("log_file", LOCUST_LOG_FILE))
        st.code(read_log_tail(log_path, lines=70) or "No log output yet.", language="text")
    with logs_col4:
        st.markdown("#### Browser Matrix Log")
        status = statuses[BROWSER_MATRIX_RUNNER_NAME]
        log_path = Path(status.get("log_file", BROWSER_MATRIX_LOG_FILE))
        st.code(read_log_tail(log_path, lines=70) or "No log output yet.", language="text")

    render_locust_insights()
    render_browser_matrix_insights()

    if auto_refresh:
        time.sleep(5)
        st.rerun()
def main() -> None:
    st.set_page_config(page_title="LoadOps Command Center", layout="wide")
    apply_style()
    render_header()

    tab_overview, tab_config, tab_threaded, tab_locust, tab_matrix, tab_comments = st.tabs(
        [
            "Overview",
            "Config Studio",
            "Threaded Journey",
            "Locust Load",
            "Browser Matrix",
            "Post Comments",
        ]
    )
    with tab_overview:
        render_overview()
    with tab_config:
        render_config_studio()
    with tab_threaded:
        render_threaded_workspace()
    with tab_locust:
        render_locust_workspace()
    with tab_matrix:
        render_matrix_workspace()
    with tab_comments:
        render_comment_workspace()


if __name__ == "__main__":
    main()
