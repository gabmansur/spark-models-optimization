"""
inspect_small_files.py
Walk the raw transactions folder on disk and summarize small-file patterns:
- file count & total bytes per day partition (day=YYYY-MM-DD)
- average file size per partition
"""

import os, sys
from collections import defaultdict

THIS_DIR = os.path.dirname(__file__)
SRC_DIR = os.path.abspath(os.path.join(THIS_DIR, ".."))
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)

from utils.common import p

def human(n):
    for unit in ["B","KB","MB","GB","TB"]:
        if n < 1024.0:
            return f"{n:0.1f} {unit}"
        n /= 1024.0
    return f"{n:0.1f} PB"

def main():
    base = p("data", "raw", "transactions")
    if not os.path.exists(base):
        print(f"Path not found: {base}. Run generate_data.py first.")
        return

    stats = defaultdict(lambda: {"files": 0, "bytes": 0})
    for root, _, files in os.walk(base):
        # Expect layout like .../transactions/day=YYYY-MM-DD/part-*.parquet
        parts = os.path.normpath(root).split(os.sep)
        day = next((x.split("=",1)[1] for x in parts if x.startswith("day=")), None)
        if day is None:
            continue
        for f in files:
            if f.endswith(".parquet"):
                fp = os.path.join(root, f)
                try:
                    sz = os.path.getsize(fp)
                    stats[day]["files"] += 1
                    stats[day]["bytes"] += sz
                except FileNotFoundError:
                    pass

    if not stats:
        print("No parquet files found under daily partitions.")
        return

    print("\n=== Small-file summary per day partition ===")
    print(f"{'day':<12} {'files':>7} {'total':>12} {'avg_file':>12}")
    for day in sorted(stats.keys()):
        files = stats[day]["files"]
        total = stats[day]["bytes"]
        avg = total / files if files else 0
        print(f"{day:<12} {files:>7} {human(total):>12} {human(avg):>12}")

    # Optional quick heuristic: warn on tiny files
    tiny_threshold = 8 * 1024 * 1024  # 8MB
    offenders = [d for d,s in stats.items() if (s["bytes"]/max(1,s["files"])) < tiny_threshold]
    if offenders:
        print("\n⚠️  Partitions with very small average files (<8MB):")
        for d in offenders:
            print(" -", d)

if __name__ == "__main__":
    main()
