# Production-Style Python Load Testing Suite

High-quality, configurable load testing and browser/device validation for **authorized** targets.

## What this project does

- Randomly iterates across a list of approved websites with weighted traffic distribution.
- Simulates mixed desktop/mobile traffic profiles via realistic user-agent rotation.
- Includes a 25-profile high-usage user-agent pool for traffic variation.
- Executes weighted user journeys (`primary`, `query`, and `health` endpoints).
- Generates Locust CSV + HTML reports.
- Runs cross-browser/device smoke checks via Playwright.
- Runs threaded browser journeys (5 parallel threads by default) with 20-30s dwell time.
- Includes a full Streamlit dashboard to monitor runners, edit config, and generate test inputs.
- Works in VS Code and Google Colab.

## What this project intentionally does not do

- No proxy rotation.
- No multi-IP evasion.
- No fake referral spoofing for abuse scenarios.

Use this only on systems you own or have explicit permission to test.

## Project structure

```text
.
├── config/
│   └── sites.example.yaml
├── loadtest/
│   ├── __init__.py
│   ├── config_builder.py
│   ├── config.py
│   ├── devices.py
│   ├── randomization.py
│   ├── runner_manager.py
│   └── user_agents.py
├── dashboard/
│   ├── app.py
│   └── styles.css
├── scripts/
│   ├── browser_matrix.py
│   ├── import_urls_to_config.py
│   ├── run_dashboard.py
│   ├── run_locust.py
│   ├── threaded_browser_journey.py
│   └── validate_config.py
├── locustfile.py
└── requirements.txt
```

## Setup (VS Code / local)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install
cp config/sites.example.yaml config/sites.yaml
```

Edit `config/sites.yaml`:

1. Add all approved target sites under `targets`.
2. Set `owner_authorization: true` only after verifying permission.
3. Tune target `weight` values to control random distribution.

Validate config:

```bash
python scripts/validate_config.py --config config/sites.yaml
```

Run dashboard:

```bash
python scripts/run_dashboard.py --host 0.0.0.0 --port 8501
```

Open [http://localhost:8501](http://localhost:8501).

## Dashboard features

- **Overview**: targets, configured pages, user-agent pool size, runner state (threaded, Locust, browser matrix).
- **Config Studio**: import URL text files, edit `config/sites.yaml`, validate configuration.
- **Runner Control**: full controls for threaded journeys, Locust headless runs, and browser/device matrix runs.
- **Threaded controls include**: threads, browser engine, links-per-site, page browse duration, scroll duration, time between scroll actions, delay between scroll and click, total site-cycle duration, scroll step size, timeout, headed/headless, cycle cap.
- **Live Monitor**: runner status table, live log tails for all runners, latest Locust metrics/charts, browser matrix pass/fail analytics.

Import from a URL text file (one URL per line):

```bash
python scripts/import_urls_to_config.py \
  --input "/absolute/path/to/urls1.txt" "/absolute/path/to/urls2.txt" \
  --output config/sites.yaml \
  --owner-authorization false
```

Then verify:

```bash
python scripts/validate_config.py --config config/sites.yaml
```

## Threaded browser journeys (your requested flow)

This runner does exactly this per thread:

1. Open one random URL from your imported list.
2. Browse 3-4 internal links found on that page.
3. Keep each page open for 20-30 seconds.
4. Move to another random URL and continue.

Default is 5 threads in parallel:

```bash
python scripts/threaded_browser_journey.py \
  --config config/sites.yaml \
  --threads 5 \
  --min-clicks 3 \
  --max-clicks 4 \
  --min-page-browse-seconds 20 \
  --max-page-browse-seconds 30 \
  --min-scroll-seconds 6 \
  --max-scroll-seconds 12 \
  --min-scroll-pause-seconds 1 \
  --max-scroll-pause-seconds 3 \
  --min-post-scroll-click-delay-seconds 2 \
  --max-post-scroll-click-delay-seconds 5
```

Optional finite run per thread:

```bash
python scripts/threaded_browser_journey.py \
  --config config/sites.yaml \
  --threads 5 \
  --max-cycles-per-thread 10
```

## Run load test (headless)

```bash
python scripts/run_locust.py \
  --config config/sites.yaml \
  --users 300 \
  --spawn-rate 20 \
  --run-time 30m
```

Outputs are written to `results/`:

- `loadtest-<timestamp>_stats.csv`
- `loadtest-<timestamp>_failures.csv`
- `loadtest-<timestamp>.html`

## Run with Locust Web UI

```bash
python scripts/run_locust.py \
  --config config/sites.yaml \
  --users 100 \
  --spawn-rate 10 \
  --run-time 20m \
  --web-ui
```

Then open [http://localhost:8089](http://localhost:8089).

## Browser/device compatibility matrix

```bash
python scripts/browser_matrix.py \
  --config config/sites.yaml \
  --browsers chromium firefox webkit \
  --paths-per-target 2
```

Results are saved to `results/browser-matrix.json`.
By default it uses 30 popular desktop/mobile device profiles (`DEFAULT_DEVICES`).

## Google Colab quick run

In Colab:

```bash
!git clone https://github.com/psainaveen12/NewsBlogTrafficGen.git
%cd <repo-folder>
!pip install -r requirements.txt
!playwright install chromium
!cp config/sites.example.yaml config/sites.yaml
```

Set `owner_authorization: true` in `config/sites.yaml`, then:

```bash
!python scripts/validate_config.py --config config/sites.yaml
!python scripts/run_locust.py --config config/sites.yaml --users 50 --spawn-rate 5 --run-time 10m
!python scripts/browser_matrix.py --config config/sites.yaml --browsers chromium
```

## Multi-country execution (recommended architecture)

Use compliant cloud runners in multiple regions, each running the same test suite against your authorized targets:

1. Deploy one Locust master endpoint.
2. Deploy worker groups per region (for example: US, EU, APAC).
3. Aggregate metrics centrally (Locust CSV + APM dashboards).

This gives region diversity without proxy/IP abuse patterns.


## Streamlit Community Cloud Deployment

1. Push this project to a GitHub repository.
2. In Streamlit Community Cloud, click **New app** and select your repo/branch.
3. Set the app entrypoint to `streamlit_app.py`.
4. Deploy. Streamlit Cloud will use:
   - `requirements.txt` for Python dependencies
   - `packages.txt` for OS libraries
   - `runtime.txt` for Python version

Notes:
- If `config/sites.yaml` is missing, the app auto-falls back to `config/sites.example.yaml`.
- Saving config from the dashboard will create/update `config/sites.yaml` at runtime.
- For heavy runners (Locust/Playwright), keep profiles conservative on Community Cloud.
