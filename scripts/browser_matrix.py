#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from time import perf_counter
from typing import Iterable
from urllib.parse import urljoin

from playwright.async_api import BrowserType, Error, Playwright, async_playwright

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from loadtest.config import TargetConfig, load_test_config
from loadtest.devices import DEFAULT_BROWSER_MATRIX_DEVICES
from loadtest.playwright_install import browser_launch_kwargs, ensure_playwright_browsers


@dataclass
class CheckResult:
    target: str
    browser: str
    device: str
    url: str
    status: int | None
    elapsed_ms: int
    ok: bool
    error: str | None = None


DEFAULT_DEVICES = list(DEFAULT_BROWSER_MATRIX_DEVICES)


def resolve_supported_devices(playwright: Playwright, devices: list[str]) -> list[str]:
    available = set(playwright.devices.keys())
    supported: list[str] = []
    missing: list[str] = []

    for device in devices:
        if device in available and device not in supported:
            supported.append(device)
        elif device not in available:
            missing.append(device)

    if missing:
        preview = ", ".join(missing[:8])
        suffix = " ..." if len(missing) > 8 else ""
        print(f"Skipping unsupported devices ({len(missing)}): {preview}{suffix}")

    if not supported:
        raise ValueError("None of the selected devices are supported by this Playwright version")

    return supported


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cross-browser and device smoke checks.")
    parser.add_argument("--config", default="config/sites.yaml", help="YAML config file path")
    parser.add_argument("--browsers", nargs="+", default=["chromium", "firefox", "webkit"])
    parser.add_argument("--devices", nargs="+", default=DEFAULT_DEVICES)
    parser.add_argument("--paths-per-target", type=int, default=2)
    parser.add_argument("--output-json", default="results/browser-matrix.json")
    return parser.parse_args()


def build_urls(target: TargetConfig, limit: int) -> list[str]:
    unique_paths = list(dict.fromkeys(target.paths + target.query_paths))
    urls = [urljoin(f"{target.base_url}/", p.lstrip("/")) for p in unique_paths[:limit]]
    if not urls:
        urls = [target.base_url]
    return urls


async def check_url(
    browser_type: BrowserType,
    playwright: Playwright,
    browser_name: str,
    device_name: str,
    target_name: str,
    url: str,
) -> CheckResult:
    device = playwright.devices[device_name]
    browser = None
    context = None

    start = perf_counter()
    try:
        launch_kwargs = browser_launch_kwargs(browser_name, headless=True)
        browser = await browser_type.launch(**launch_kwargs)
        context = await browser.new_context(**device)
        page = await context.new_page()
        response = await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        elapsed_ms = int((perf_counter() - start) * 1000)
        status = response.status if response else None
        ok = bool(status and 200 <= status < 400)
        result = CheckResult(
            target=target_name,
            browser=browser_name,
            device=device_name,
            url=url,
            status=status,
            elapsed_ms=elapsed_ms,
            ok=ok,
            error=None if ok else f"Unexpected status: {status}",
        )
    except Error as exc:
        elapsed_ms = int((perf_counter() - start) * 1000)
        result = CheckResult(
            target=target_name,
            browser=browser_name,
            device=device_name,
            url=url,
            status=None,
            elapsed_ms=elapsed_ms,
            ok=False,
            error=str(exc),
        )
    finally:
        if context is not None:
            try:
                await context.close()
            except Error:
                pass
        if browser is not None:
            try:
                await browser.close()
            except Error:
                pass
    return result


async def run_checks(args: argparse.Namespace) -> list[CheckResult]:
    cfg = load_test_config(args.config)
    if not cfg.owner_authorization:
        raise ValueError("owner_authorization must be true before running checks")

    targets = cfg.targets

    results: list[CheckResult] = []
    async with async_playwright() as playwright:
        selected_devices = resolve_supported_devices(playwright, args.devices)
        checks: list[tuple[str, str, str, str]] = []
        for target in targets:
            for url in build_urls(target, args.paths_per_target):
                for browser in args.browsers:
                    for device in selected_devices:
                        checks.append((target.name, browser, device, url))

        for target_name, browser_name, device_name, url in checks:
            browser_type = getattr(playwright, browser_name, None)
            if browser_type is None:
                results.append(
                    CheckResult(
                        target=target_name,
                        browser=browser_name,
                        device=device_name,
                        url=url,
                        status=None,
                        elapsed_ms=0,
                        ok=False,
                        error=f"Unsupported browser: {browser_name}",
                    )
                )
                continue
            result = await check_url(
                browser_type=browser_type,
                playwright=playwright,
                browser_name=browser_name,
                device_name=device_name,
                target_name=target_name,
                url=url,
            )
            results.append(result)
    return results


def print_summary(results: Iterable[CheckResult]) -> None:
    rows = list(results)
    passed = sum(1 for row in rows if row.ok)
    failed = len(rows) - passed
    print(f"Total checks: {len(rows)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    if failed:
        print("Failures:")
        for row in rows:
            if not row.ok:
                print(f"- {row.target} | {row.browser} | {row.device} | {row.url} | {row.error}")


def write_json(path: str | Path, results: Iterable[CheckResult]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = [asdict(result) for result in results]
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    ok, message = ensure_playwright_browsers(args.browsers, install_missing=True)
    if not ok:
        raise SystemExit(message)
    if message and "installed" in message.lower():
        print(message)
    results = asyncio.run(run_checks(args))
    write_json(args.output_json, results)
    print_summary(results)


if __name__ == "__main__":
    main()
