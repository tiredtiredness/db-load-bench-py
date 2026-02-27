import json
from pathlib import Path
from .chart_data import ChartStore, MethodRun

RESULTS_FILE = Path("results.json")


def save_results(store: ChartStore) -> None:
    data = {
        db_type: {
            method: [
                {
                    "rows": run.rows,
                    "elapsed": run.elapsed,
                    "rps": run.rps,
                    "batch_size": run.batch_size,
                }
                for run in runs
            ]
            for method, runs in methods.items()
        }
        for db_type, methods in store.items()
    }
    RESULTS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


def load_results() -> ChartStore:
    if not RESULTS_FILE.exists():
        return {}

    try:
        data = json.loads(RESULTS_FILE.read_text(encoding="utf-8"))
        return {
            db_type: {
                method: [
                    MethodRun(
                        rows=r["rows"],
                        elapsed=r["elapsed"],
                        rps=r["rps"],
                        batch_size=r.get("batch_size"),
                    )
                    for r in runs
                ]
                for method, runs in methods.items()
            }
            for db_type, methods in data.items()
        }
    except (json.JSONDecodeError, KeyError):
        return {}


def clear_results_file() -> None:
    if RESULTS_FILE.exists():
        RESULTS_FILE.unlink()
