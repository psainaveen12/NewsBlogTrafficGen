#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${1:-/content/NEWSBlogTrafficTest}"
OWNER_AUTHORIZATION="${OWNER_AUTHORIZATION:-false}"
PLAYWRIGHT_BROWSERS="${PLAYWRIGHT_BROWSERS:-chromium firefox webkit}"

cd "$PROJECT_ROOT"

echo "[bootstrap] Project root: $PROJECT_ROOT"
python3 -m pip install --upgrade pip setuptools wheel
python3 -m pip install -r requirements.txt
python3 -m pip install -r NewsBlogTraffic_Colab/requirements.colab.txt

# Install browser binaries and OS deps required by Playwright.
python3 -m playwright install --with-deps $PLAYWRIGHT_BROWSERS

python3 NewsBlogTraffic_Colab/colab_prepare_runtime.py   --project-root "$PROJECT_ROOT"   --owner-authorization "$OWNER_AUTHORIZATION"

echo "[bootstrap] Completed."
