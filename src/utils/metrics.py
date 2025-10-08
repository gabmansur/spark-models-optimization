"""
metrics.py
----------
Runtime logging + clean Before->After (p95) chart.

- log_run(): append a row per job execution to ops/job_runs.csv
- build_report(): compute p50/p95 per model and render Markdown + chart
"""

import os, sys, uuid
from datetime import datetime, timezone
import pandas as pd
import matplotlib.pyplot as plt

# Ensure "src" is importable when run as a script
THIS_DIR = os.path.dirname(__file__)
SRC_DIR = os.path.abspath(os.path.join(THIS_DIR, ".."))
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)

from utils.common import p

OPS = p("ops")
os.makedirs(OPS, exist_ok=True)
RUNS_CSV = os.path.join(OPS, "job_runs.csv")


def log_run(model_name: str, start_ts: float, end_ts: float, status: str, cost_eur: float = None):
    """Append execution info to ops/job_runs.csv."""
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
    hdr = not os.path.exists(RUNS_CSV)
    pd.DataFrame([row]).to_csv(RUNS_CSV, mode="a", header=hdr, index=False)


def _family(name: str) -> str:
    return name.replace("_optimized", "").replace("_baseline", "")


def build_report():
    """Create ops/report.md and ops/p95_chart.png from recorded runs."""
    if not os.path.exists(RUNS_CSV):
        print("No runs yet. Execute baseline/optimized first.")
        return

    df = pd.read_csv(RUNS_CSV, parse_dates=["start_ts", "end_ts"])
    succ = df[df["status"].astype(str).str.startswith("SUCCESS")]
    if succ.empty:
        print("No successful runs to report.")
        return

    g = succ.groupby("model_name")["duration_seconds"]
    summary = pd.DataFrame({
        "runs": g.count(),
        "avg_s": g.mean(),
        "p50_s": g.quantile(0.50),
        "p95_s": g.quantile(0.95),
    }).reset_index()

    summary["family"] = summary["model_name"].apply(_family)
    summary["type"] = summary["model_name"].apply(lambda n: "optimized" if n.endswith("_optimized") else "baseline")

    pivot = summary.pivot_table(index="family", columns="type", values="p95_s", aggfunc="first").reset_index()
    pivot = pivot.rename(columns={"family": "model"}).fillna("")

    report_path = os.path.join(OPS, "report.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("# Runtime Report (p95)\n\n")
        f.write("Durations in seconds. Lower is better.\n\n")
        f.write("## Per-job summary\n\n")
        f.write(summary.to_markdown(index=False))
        f.write("\n\n## Before -> After (p95 seconds)\n\n")
        f.write(pivot.to_markdown(index=False))

    # ----------- Improved chart aesthetics -----------
    if not pivot.empty and ("baseline" in pivot.columns or "optimized" in pivot.columns):
        import numpy as np

        models = pivot["model"].astype(str).tolist()
        baseline_vals = pivot.get("baseline", pd.Series([float("nan")] * len(models))).astype(float).tolist()
        optimized_vals = pivot.get("optimized", pd.Series([float("nan")] * len(models))).astype(float).tolist()

        y = np.arange(len(models))
        bar_height = 0.25
        group_gap = 0.6
        y_base = y + group_gap / 2   # baseline ABOVE
        y_opt  = y - group_gap / 2   # optimized BELOW

        fig, ax = plt.subplots(figsize=(9, 0.9 + 0.8 * len(models)))

        # Bars (baseline first, then optimized)
        bars_base = ax.barh(y_base, baseline_vals, height=bar_height, label="baseline")
        bars_opt  = ax.barh(y_opt, optimized_vals, height=bar_height, label="optimized")

        ax.grid(axis="x", linestyle="--", alpha=0.35)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)

        ax.set_xlabel("p95 duration (s)")
        ax.set_yticks(y)
        ax.set_yticklabels(models)
        ax.set_title("Before -> After (p95) per model")

        finite_vals = [v for v in (baseline_vals + optimized_vals) if not (np.isnan(v) or v is None)]
        max_x = max(finite_vals + [1.0])
        ax.set_xlim(0, max_x * 1.25)

        # Labels at end of bars
        def add_labels(bars):
            for b in bars:
                w = b.get_width()
                if w == w:
                    ax.text(w + max_x * 0.02, b.get_y() + b.get_height() / 2, f"{w:.1f}s",
                            va="center", ha="left")

        add_labels(bars_base)
        add_labels(bars_opt)

        # Show improvement percentage if 1 pair
        if len(models) == 1 and not (np.isnan(baseline_vals[0]) or np.isnan(optimized_vals[0])) and baseline_vals[0] > 0:
            pct = max(0.0, (1.0 - optimized_vals[0] / baseline_vals[0]) * 100.0)
            ax.annotate(f"{pct:.0f}% faster", xy=(max_x * 0.55, y[0]), xytext=(0, -25),
                        textcoords="offset points", ha="center", va="bottom")

        # Legend with baseline first
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles, labels, loc="upper right")

        plt.tight_layout()
        chart_path = os.path.join(OPS, "p95_chart.png")
        plt.savefig(chart_path)
        plt.close(fig)

    print(f"Report written to {report_path}")
    if os.path.exists(os.path.join(OPS, "p95_chart.png")):
        print("Chart saved to ops/p95_chart.png")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "report":
        build_report()
    else:
        print("Usage: python src/utils/metrics.py report")
