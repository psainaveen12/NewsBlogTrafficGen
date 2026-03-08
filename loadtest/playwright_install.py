from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Iterable


def _normalize_browsers(browser_names: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    for name in browser_names:
        browser = str(name).strip().lower()
        if browser and browser not in normalized:
            normalized.append(browser)
    return normalized


def ensure_playwright_browsers(
    browser_names: Iterable[str],
    install_missing: bool = True,
) -> tuple[bool, str]:
    try:
        from playwright.sync_api import sync_playwright
    except ModuleNotFoundError:
        return False, (
            "Playwright Python package is not installed. "
            "Run: pip install -r requirements.txt"
        )

    browsers = _normalize_browsers(browser_names)
    if not browsers:
        return True, "No browser requested."

    missing: list[str] = []

    with sync_playwright() as playwright:
        for browser_name in browsers:
            browser_type = getattr(playwright, browser_name, None)
            if browser_type is None:
                return False, f"Unsupported Playwright browser: {browser_name}"
            executable = Path(browser_type.executable_path)
            if not executable.exists():
                missing.append(browser_name)

    if not missing:
        return True, "Playwright browser binaries are already available."

    if not install_missing:
        return False, (
            "Missing Playwright browser binaries: "
            + ", ".join(missing)
            + ". Run: playwright install "
            + " ".join(missing)
        )

    cmd = [sys.executable, "-m", "playwright", "install", *missing]
    result = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        details = (result.stderr or result.stdout or "").strip()
        if details:
            details = details[-1200:]
        return False, (
            "Playwright browser install failed for "
            + ", ".join(missing)
            + (f". Details: {details}" if details else "")
        )

    # Re-check after install.
    with sync_playwright() as playwright:
        still_missing: list[str] = []
        for browser_name in browsers:
            browser_type = getattr(playwright, browser_name, None)
            if browser_type is None:
                still_missing.append(browser_name)
                continue
            executable = Path(browser_type.executable_path)
            if not executable.exists():
                still_missing.append(browser_name)

    if still_missing:
        return False, (
            "Playwright install completed but binaries are still missing for: "
            + ", ".join(still_missing)
        )

    return True, "Installed missing Playwright browsers: " + ", ".join(missing)
