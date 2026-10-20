import sys
import time
from pyspark.sql import SparkSession, functions as F

src = sys.argv[1]   # the CSV
dst = sys.argv[2]   # the Parquet folder

spark = (
    SparkSession.builder
    .appName("partd-bronze")
    .master("local[*]")
    .config("spark.driver.memory", "6g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

t0 = time.time()
df = (
    spark.read.option("header", True).csv(src)        # all strings, no inference
    .withColumn("_source_file", F.lit(src.split("/")[-1]))
    .withColumn("_ingested_at", F.current_timestamp())
)
df.write.mode("overwrite").parquet(dst)               # snappy compression by default
print(f"Wrote bronze Parquet in {time.time() - t0:.1f}s")

check = spark.read.parquet(dst)
print(f"Rows in Parquet: {check.count():,}")
spark.stop()