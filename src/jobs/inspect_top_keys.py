"""
inspect_top_keys.py
-------------------
Summarize the most frequent customer_id values in transactions, quantify skew,
and save outputs to ops/diagnostics/.
"""

import os, sys
from pyspark.sql import functions as F

# Make 'src' importable when running as a script
THIS_DIR = os.path.dirname(__file__)
SRC_DIR = os.path.abspath(os.path.join(THIS_DIR, ".."))
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)

from utils.common import get_spark, p

def main():
    spark = get_spark("inspect_top_keys")

    tx = spark.read.parquet(p("data","raw","transactions"))
    total = tx.count()

    top = (tx.groupBy("customer_id")
             .count()
             .orderBy(F.desc("count"))
             .limit(10)
             .withColumn("pct", F.round(F.col("count")/F.lit(total)*100, 2)))

    print(f"Total rows: {total}")
    top.show(truncate=False)

    # Hot key (first row)
    row0 = top.first()
    hot_id = row0["customer_id"]
    hot_share = float(row0["pct"])

    # Save artifacts
    out_dir = p("ops","diagnostics")
    os.makedirs(out_dir, exist_ok=True)

    # CSV
    top.toPandas().to_csv(os.path.join(out_dir, "top_keys.csv"), index=False)

    # Markdown
    md_path = os.path.join(out_dir, "top_keys.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("## Top Keys Summary (transactions)\n\n")
        f.write(f"- Total rows: **{total:,}**\n")
        f.write(f"- Hot key: **customer_id = {hot_id}** (~**{hot_share:.2f}%** of all rows)\n\n")
        f.write("| customer_id | count | pct |\n")
        f.write("|---:|---:|---:|\n")
        for r in top.collect():
            f.write(f"| {r['customer_id']} | {r['count']:,} | {r['pct']:.2f}% |\n")

    print(f"\nSaved: {os.path.relpath(md_path)} and top_keys.csv")
    spark.stop()

if __name__ == "__main__":
    main()