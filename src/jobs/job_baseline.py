"""
job_baseline.py
---------------
An intentionally naive/slow version of the pipeline to create a performance baseline.

Anti-patterns included on purpose:
- Python UDF (slow; bypasses Catalyst optimizations)
- Regular join on a skewed key (hot customer_id=0 → slow stage)
- Writes many small files (over-partitioned output)
"""

import os, sys, time
from pyspark.sql import functions as F, types as T

# Make 'src' importable when running as a script
THIS_DIR = os.path.dirname(__file__)
SRC_DIR = os.path.abspath(os.path.join(THIS_DIR, ".."))
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)

from utils.common import get_spark, p
from utils.metrics import log_run

def python_udf_bucket(amount: float) -> str:
    """Categorize amounts via a Python UDF (slow on purpose)."""
    if amount is None:
        return "unknown"
    if amount < 20: return "low"
    if amount < 100: return "mid"
    return "high"

def main():
    spark = get_spark("job_baseline")
    start = time.time()
    status = "SUCCESS"

    try:
        raw_dir = p("data","raw")

        # Read small dimension + large fact (lots of small files across days)
        customers = spark.read.parquet(os.path.join(raw_dir,"customers"))
        tx = spark.read.parquet(os.path.join(raw_dir,"transactions"))  # reads all partitions

        # 1) Use a Python UDF for bucketing (this is intentionally suboptimal)
        from pyspark.sql.functions import udf
        bucket_udf = udf(python_udf_bucket, T.StringType())
        tx2 = tx.withColumn("bucket", bucket_udf(F.col("amount")))

        # 2) Naive join on a skewed key (customer_id=0 is a hotspot)
        joined = tx2.join(customers, "customer_id", "left")

        # 3) Aggregate to daily metrics (no smart partitioning)
        out = (
            joined
            .withColumn("event_date", F.to_date("event_ts"))
            .groupBy("event_date","segment","bucket")
            .agg(F.count("*").alias("tx_count"), F.sum("amount").alias("revenue"))
        )

        # 4) Write MANY small files again (keeps things slow/expensive)
        (out.repartition(120)
            .write.mode("overwrite")
            .parquet(p("data","bronze","daily_sales_naive")))

    except Exception as e:
        status = f"FAIL:{e.__class__.__name__}"
        raise
    finally:
        end = time.time()
        # Rough cost estimate for the run (set CLUSTER_EUR_PER_HOUR env if you want)
        eur_per_hour = float(os.getenv("CLUSTER_EUR_PER_HOUR", "2.50"))
        cost_eur = (end - start) / 3600.0 * eur_per_hour

        # Log run metrics so we can compute p95 later
        log_run("daily_sales_baseline", start, end, status, cost_eur)
        spark.stop()

if __name__ == "__main__":
    main()
