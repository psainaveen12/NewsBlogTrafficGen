from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


COMMAND_TO_SCRIPT = {
    "dashboard": "scripts/run_dashboard.py",
    "threaded": "scripts/threaded_browser_journey.py",
    "locust": "scripts/run_locust.py",
    "matrix": "scripts/browser_matrix.py",
    "validate": "scripts/validate_config.py",
    "import-urls": "scripts/import_urls_to_config.py",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "NEWSBlogTrafficTest launcher. "
            "Example: python . dashboard --host 0.0.0.0 --port 8501"
        )
    )
    parser.add_argument(
        "command",
        nargs="?",
        default="dashboard",
        choices=sorted(COMMAND_TO_SCRIPT.keys()),
        help="Command to run (default: dashboard)",
    )
    parser.add_argument(
        "command_args",
        nargs=argparse.REMAINDER,
        help="Arguments forwarded to the selected command",
    )
    return parser.parse_args()


def resolve_python(project_root: Path) -> str:
    venv_python = project_root / ".venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parent
    script_path = project_root / COMMAND_TO_SCRIPT[args.command]
    python_bin = resolve_python(project_root)
    cmd = [python_bin, str(script_path), *args.command_args]
    subprocess.run(cmd, cwd=str(project_root), check=True)


if __name__ == "__main__":
    main()
