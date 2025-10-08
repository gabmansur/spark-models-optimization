from src.utils.common import get_spark, p

spark = get_spark("inspect_customers")

df = spark.read.parquet(p("data", "raw", "customers"))

print("Customer schema:")
df.printSchema()

print("Sample rows:")
df.show(5, truncate=False)

spark.stop()