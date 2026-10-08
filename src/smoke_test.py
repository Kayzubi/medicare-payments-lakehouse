import time
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Smoke Test")
    .master("local[*]")
    .config("spark.driver.memory", "6g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

jvm = spark.sparkContext._jvm
print("Spark version:", spark.version)
print("Java version:", jvm.System.getProperty("java.version"))
print("Cores used:", spark.sparkContext.defaultParallelism)

t0 = time.time()
n = spark.range(10_000_000).selectExpr("id % 7 as k").groupBy("k").count().count()
print(f"Test job: {n} groups in {time.time() - t0:.1f}s")

spark.stop()