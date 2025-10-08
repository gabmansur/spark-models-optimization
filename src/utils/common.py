"""
common.py
---------
Creates a SparkSession with your local config and provides simple path helpers.
Used by all jobs to start Spark the same way and to build project-relative paths.
"""

import os
from pyspark.sql import SparkSession

def project_root():
    """Return the absolute path to the project root (two levels up from /src/utils)."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

def p(*parts):
    """
    Join one or more path parts to the project root.
    Example: p("data","raw") -> /.../spark-optimization-hands-on/data/raw
    """
    return os.path.join(project_root(), *parts)

def get_spark(app_name: str):
    """
    Build (or get) a SparkSession with local master by default and apply the
    settings from conf/spark_local.conf. This keeps runs consistent.
    """
    # Allow overriding the master (e.g., connect to a real cluster) via env
    master = os.environ.get("SPARK_MASTER", "local[*]")

    # Start building the SparkSession
    builder = (SparkSession.builder
               .appName(app_name)
               .master(master))

    # Load local Spark config (AQE, skew handling, etc.)
    conf_file = p("conf", "spark_local.conf")
    if os.path.exists(conf_file):
        with open(conf_file) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    builder = builder.config(k.strip(), v.strip())

    # Create or reuse the session
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # Quieter logs
    return spark
