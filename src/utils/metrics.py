"""
metrics.py
----------
Lightweight runtime logging and reporting utilities.

- log_run(): append one row per job execution to ops/job_runs.csv
- build_report(): compute p50/p95 per job and render a Before→After table + chart
"""

import os, sys, uuid
from datetime import datetime, timezone
import pandas as pd
import matplotlib.pyplot as plt

# Ensure "src" is on the Python path when run directly (python src/utils/metrics.py report)
THIS_DIR = os.path.dirname(__file__)
SRC_DIR = os.path.abspath(os.path.join(THIS_DIR, ".."))
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)

from utils.common import p  # path helper

OPS = p("ops")
os.makedirs(OPS, exist_ok=True)
RUNS_CSV = os.path.join(OPS, "job_runs.csv")

def log_run(model_name: str, start_ts: float, end_ts: float, status: str, cost_eur: float = None):
    """
    Append a single job execution row to ops/job_runs.csv.
    - model_name: logical job name (e.g., 'daily_sales_baseline')
    - start_ts / end_ts: UNIX timestamps
    - status: 'SUCCESS' or 'FAIL:<ErrorType>'
    - cost_eur: optional estimated cost per run
    """
    duration = float(end_ts - start_ts)
    row = {
        "model_name": model_name,
        "run_id": str(uuid.uuid4()),
        "start_ts": datetime.fromtimestamp(start_ts, tz=timezone.utc).isoformat(),
        "end_ts": datetime.fromtimestamp(end_ts, tz=timezone.utc).isoformat(),
        "duration_seconds": duration,
        "status": status,
        "cost_eur": cost_eur if cost_eur is not None else ""
    }
    hdr = not os.path.exists(RUNS_CSV)  # write header only for first append
    pd.DataFrame([row]).to_csv(RUNS_CSV, mode="a", header=hdr, index=False)

def build_report():
    """
    Read ops/job_runs.csv, compute p50/p95 per model, and produce:
    - ops/report.md (markdown summary + Before→After table)
    - ops/p95_chart.png (bar chart comparing baseline vs optimized p95)
    """
    if not os.path.exists(RUNS_CSV):
        print("No runs yet. Execute baseline/optimized first.")
        return

    df = pd.read_csv(RUNS_CSV, parse_dates=["start_ts","end_ts"])
    # Focus on successful runs for timing stats
    succ = df[df["status"].astype(str).str.startswith("SUCCESS")]
    if succ.empty:
        print("No successful runs to report.")
        return

    # Group by model name and compute aggregates
    g = succ.groupby("model_name")["duration_seconds"]
    summary = pd.DataFrame({
        "runs": g.count(),
        "avg_s": g.mean(),
        "p50_s": g.quantile(0.50),         # median runtime
        "p95_s": g.quantile(0.95),         # “slow-day line”
    }).reset_index()

    # Derive a family name so 'daily_sales_baseline' and 'daily_sales_optimized' are grouped
    def fam(name: str) -> str:
        return name.replace("_optimized","")
    summary["family"] = summary["model_name"].apply(fam)
    summary["type"] = summary["model_name"].apply(lambda n: "optimized" if n.endswith("_optimized") else "baseline")

    # Pivot so we can see baseline vs optimized p95 side-by-side
    pivot = summary.pivot_table(index="family", columns="type", values="p95_s", aggfunc="first")
    pivot = pivot.reset_index().rename(columns={"family":"model"}).fillna("")

    # Write Markdown report
    report_path = os.path.join(OPS, "report.md")
    with open(report_path, "w") as f:
        f.write("# Runtime Report (p95)\n\n")
        f.write("Durations in seconds. Lower is better.\n\n")
        f.write("## Per-job summary\n\n")
        f.write(summary.to_markdown(index=False))
        f.write("\n\n## Before → After (p95 seconds)\n\n")
        f.write(pivot.to_markdown(index=False))

    # Build the bar chart (Before→After p95)
    if not pivot.empty and "baseline" in pivot and "optimized" in pivot:
        import numpy as np
        labels = pivot["model"].tolist()
        x = np.arange(len(labels))
        b = pivot["baseline"].astype(float).tolist()
        o = pivot["optimized"].astype(float).tolist()
        width = 0.35

        plt.figure()
        plt.bar(x - width/2, b, width, label="baseline")   # before
        plt.bar(x + width/2, o, width, label="optimized")  # after
        plt.xticks(x, labels, rotation=45, ha="right")
        plt.ylabel("p95 duration (s)")
        plt.title("Before → After (p95)")
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(OPS, "p95_chart.png"))

    print(f"Report written to {report_path}")
    if os.path.exists(os.path.join(OPS, 'p95_chart.png')):
        print("Chart saved to ops/p95_chart.png")

if __name__ == "__main__":
    # CLI usage: python src/utils/metrics.py report
    if len(sys.argv) > 1 and sys.argv[1] == "report":
        build_report()
    else:
        print("Usage: python src/utils/metrics.py report")