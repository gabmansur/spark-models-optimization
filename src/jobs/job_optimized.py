"""
job_optimized.py
----------------
A tuned version of the pipeline that demonstrates common Spark optimizations:

- Replace Python UDF with Spark built-ins (faster; Catalyst-optimized)
- Handle skew via key salting (split heavy key across SALT_N buckets)
- Broadcast small dimension table (avoid huge shuffles)
- Enable AQE + skew handling (let Spark adapt plan at runtime)
- Write partitioned outputs with fewer, larger files (faster downstream IO)
"""

import os, sys, time
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast, when, col, rand, lit, concat_ws, explode, array

# Make 'src' importable when running as a script
THIS_DIR = os.path.dirname(__file__)
SRC_DIR = os.path.abspath(os.path.join(THIS_DIR, ".."))
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)

from utils.common import get_spark, p
from utils.metrics import log_run

SALT_N = 16  # number of salt buckets for the heavy key; tune up/down per skew

def main():
    spark = get_spark("job_optimized")

    # Make sure AQE and skew handling are ON (also configured in conf file)
    spark.conf.set("spark.sql.adaptive.enabled","true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled","true")

    start = time.time()
    status = "SUCCESS"

    try:
        raw_dir = p("data","raw")

        # Read small dimension + large fact (same inputs as baseline)
        customers = spark.read.parquet(os.path.join(raw_dir,"customers"))
        tx = spark.read.parquet(os.path.join(raw_dir,"transactions"))

        # 1) Replace Python UDF with built-in expressions (fast & optimizable)
        tx2 = tx.withColumn(
            "bucket",
            when(col("amount") < 20, "low").when(col("amount") < 100, "mid").otherwise("high")
        )

        # 2) Skew fix: SALT the heavy key only (customer_id==0) on the left side.
        left = (
            tx2
            .withColumn("salt", when(col("customer_id") == 0, (rand() * SALT_N).cast("int")).otherwise(lit(0)))
            .withColumn("customer_salted", concat_ws("_", col("customer_id"), col("salt")))
        )

        # 3) Prepare the dimension with replicated salts for the hot key (right side)
        right = (
            customers
            .withColumn("salt_rep", explode(array([lit(i) for i in range(SALT_N)])))
            .withColumn(
                "customer_salted",
                concat_ws("_", col("customer_id"), when(col("customer_id") == 0, col("salt_rep")).otherwise(lit(0)))
            )
            .drop("salt_rep")
        )

        # 4) Broadcast the small dimension to avoid a big shuffle during the join
        joined = left.join(
            broadcast(right.select("customer_id","segment","signup_date","customer_salted")),
            "customer_salted",
            "left"
        ).drop("customer_salted")

        # 5) Aggregate to daily metrics
        out = (
            joined
            .withColumn("event_date", F.to_date("event_ts"))
            .groupBy("event_date","segment","bucket")
            .agg(F.count("*").alias("tx_count"), F.sum("amount").alias("revenue"))
        )

        # 6) Write partitioned outputs (fewer, larger files per date partition)
        (out
         .repartition("event_date")           # cluster data by partition column
         .write.mode("overwrite")
         .partitionBy("event_date")           # write layout: .../event_date=YYYY-MM-DD/
         .parquet(p("data","silver","daily_sales_optimized")))

    except Exception as e:
        status = f"FAIL:{e.__class__.__name__}"
        raise
    finally:
        end = time.time()
        eur_per_hour = float(os.getenv("CLUSTER_EUR_PER_HOUR", "2.50"))
        cost_eur = (end - start) / 3600.0 * eur_per_hour

        # Log the improved runtime so we can compare p95 Before→After
        log_run("daily_sales_optimized", start, end, status, cost_eur)
        spark.stop()

if __name__ == "__main__":
    main()