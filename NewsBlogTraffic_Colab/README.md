# NewsBlogTraffic Colab Package

This folder contains everything needed to run the project in Google Colab.

## What is included

- `bootstrap_colab.sh`: installs Python dependencies + Playwright browsers and prepares runtime config.
- `colab_prepare_runtime.py`: normalizes local/macOS-specific paths into Colab-safe relative paths.
- `run_all_smoke.py`: runs a lightweight end-to-end smoke test of main runners.
- `NewsBlogTraffic_Colab.ipynb`: ready notebook with setup, dashboard launch, and runner commands.
- `requirements.colab.txt`: Colab-specific extra dependencies.

## Quick start (Colab)

1. Open `NewsBlogTraffic_Colab.ipynb` in Google Colab.
2. Set `REPO_URL` in the first code cell.
3. Run cells in order.

## Manual commands (if needed)

```bash
cd /content/NEWSBlogTrafficTest
bash NewsBlogTraffic_Colab/bootstrap_colab.sh /content/NEWSBlogTrafficTest
python scripts/validate_config.py --config config/sites.yaml
python scripts/run_dashboard.py --host 0.0.0.0 --port 8501 --headless
```

## Notes

- `owner_authorization` is set to `false` by default in Colab prep.
- Change it with:

```bash
python NewsBlogTraffic_Colab/colab_prepare_runtime.py   --project-root /content/NEWSBlogTrafficTest   --owner-authorization true
```
