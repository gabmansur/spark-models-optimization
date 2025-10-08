"""
inspect_transactions.py
Quick visibility into the generated transactions dataset:
- print schema and a sample
- quantify skew (hot key share, top keys)
- show per-day volumes
- count small files per partition folder on disk
"""

import os
import sys
from collections import defaultdict

# ensure project root is on sys.path when running as a script
THIS_FILE = os.path.abspath(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(THIS_FILE, "..", "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.common import get_spark, p
from pyspark.sql import functions as F

def count_parquet_files_by_day(base_dir):
    """
    Walk data/raw/transactions/day=YYYY-MM-DD and count part files per day.
    This runs on the local filesystem, not through Spark.
    """
    result = defaultdict(int)
    tx_dir = os.path.join(base_dir, "transactions")
    if not os.path.exists(tx_dir):
        return {}

    for entry in os.scandir(tx_dir):
        if entry.is_dir() and entry.name.startswith("day="):
            day = entry.name.split("day=", 1)[1]
            # count *.parquet under this dir
            file_count = 0
            for root, _dirs, files in os.walk(entry.path):
                file_count += sum(1 for f in files if f.endswith(".parquet"))
            result[day] = file_count
    return dict(result)

def main():
    spark = get_spark("inspect_transactions")

    tx_path = p("data", "raw", "transactions")
    print(f"\nReading: {tx_path}\n")

    df = spark.read.parquet(tx_path)

    print("Schema:")
    df.printSchema()

    print("\nSample rows:")
    df.show(10, truncate=False)

    # basic counts
    total = df.count()
    print(f"\nTotal rows: {total:,}")

    # hot key share: customer_id = 0
    hot_key = 0
    hot_cnt = df.filter(F.col("customer_id") == hot_key).count()
    hot_share = (hot_cnt / total) * 100 if total else 0
    print(f"Hot key customer_id={hot_key} count: {hot_cnt:,}  share: {hot_share:.2f}%")

    # top 10 keys by row count
    print("\nTop 10 customer_ids by row count:")
    top_keys = (
        df.groupBy("customer_id")
          .count()
          .orderBy(F.col("count").desc())
          .limit(10)
    )
    top_keys.show(truncate=False)

    # per-day volume
    print("\nRows per day partition:")
    by_day = (
        df.groupBy("day")
          .count()
          .orderBy("day")
    )
    by_day.show(20, truncate=False)

    # on-disk file counts per day
    files_by_day = count_parquet_files_by_day(p("data", "raw"))
    if files_by_day:
        print("\nParquet file count per day on disk:")
        for day, n in sorted(files_by_day.items()):
            print(f"  {day}: {n} files")
    else:
        print("\nNo on-disk parquet file counts found. check data/raw/transactions exists.")

    print("\nTip: open Spark UI at http://localhost:4040 while running heavy jobs to see stages, tasks, and skew.")
    spark.stop()

if __name__ == "__main__":
    main()