"""
generate_data.py
----------------
Create dummy ‘customers’ (small dimension) and ‘transactions’ (large, skewed)
and write them to /data/raw as parquet. Also intentionally writes many small files
to simulate a common Spark performance problem.
"""

import os, sys
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F

# Ensure we can import from src/utils when running as a script
THIS_DIR = os.path.dirname(__file__)
SRC_DIR = os.path.abspath(os.path.join(THIS_DIR, ".."))
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)

from utils.common import get_spark, p

# Tweak volumes if your laptop is slow
N_CUSTOMERS = 10000
N_TRANSACTIONS = 120_000   # increase to stress, decrease to run faster
SKEWED_CUSTOMER_SHARE = 0.20  # 20% of tx go to hot key: customer_id=0

def main():
    spark = get_spark("generate_data")

    raw_dir = p("data", "raw")
    os.makedirs(raw_dir, exist_ok=True)

    # Build a small customer dimension table (we will broadcast join this later)
    customers = (
        spark.range(0, N_CUSTOMERS)
             .withColumnRenamed("id", "customer_id")
             .withColumn(
                 "segment",
                 F.when((F.col("customer_id") % 10) < 2, F.lit("VIP")).otherwise("STD")
             )
             .withColumn(
                 "signup_date",
                 F.expr("date_sub(current_date(), cast(rand()*1800 as int))")
             )
    )
    customers.write.mode("overwrite").parquet(os.path.join(raw_dir, "customers"))

    # Generate 14 daily partitions of transactions, with skew to customer_id=0
    start = datetime.now(timezone.utc) - timedelta(days=14)
    rows_per_day = N_TRANSACTIONS // 14

    for d in range(14):
        day = (start + timedelta(days=d)).date().isoformat()

        skew_count = int(rows_per_day * SKEWED_CUSTOMER_SHARE)
        norm_count = rows_per_day - skew_count

        # Skewed part: many rows for customer_id=0; random amounts and timestamps
        skew = (
            spark.range(0, skew_count)
                 .withColumn("customer_id", F.lit(0))
                 .withColumn("amount", (F.rand() * 200).cast("double"))
                 # Generate event_ts dynamically (0–12h random offset)
                 .withColumn("base_ts", F.lit(f"{day} 12:00:00"))
                 .withColumn("rand_seconds", (F.rand(seed=42) * 43200).cast("int"))
                 .withColumn(
                     "event_ts",
                     F.to_timestamp(
                         F.from_unixtime(
                             F.unix_timestamp(F.col("base_ts")) + F.col("rand_seconds")
                         )
                     )
                 )
                 .drop("base_ts", "rand_seconds")
        )

        # Normal part: rows spread across many customers
        norm = (
            spark.range(0, norm_count)
                 .withColumn("customer_id", (F.rand() * (N_CUSTOMERS - 1) + 1).cast("int"))
                 .withColumn("amount", (F.rand() * 200).cast("double"))
                 .withColumn("base_ts", F.lit(f"{day} 00:00:00"))
                 .withColumn("rand_seconds", (F.rand(seed=123) * 86400).cast("int"))
                 .withColumn(
                     "event_ts",
                     F.to_timestamp(
                         F.from_unixtime(
                             F.unix_timestamp(F.col("base_ts")) + F.col("rand_seconds")
                         )
                     )
                 )
                 .drop("base_ts", "rand_seconds")
        )

        # Merge both into one DataFrame
        daily = skew.unionByName(norm)

        # INTENTIONALLY over-partition to create many small files
        path = os.path.join(raw_dir, "transactions", f"day={day}")
        daily.repartition(120).write.mode("overwrite").parquet(path)

    print(f"Wrote customers + transactions (skew & small files) under {raw_dir}")
    spark.stop()

if __name__ == "__main__":
    main()