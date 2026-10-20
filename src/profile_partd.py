import sys
import time
from pyspark.sql import SparkSession, functions as F


path = sys.argv[1]

spark = (
    SparkSession.builder
    .appName("partd_profile")
    .master("local[*]")
    .config("spark.driver.memory", "6g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Every column read as a string. No inference, no extra pass over the file.
df = spark.read.option("header", True).csv(path)
print(f"Columns: {len(df.columns)}")
print(f"Partitions: {df.rdd.getNumPartitions()}")


# Pass 1: row count, nulls per column and distinct NPIs together.
t0 = time.time()
aggs = [F.count(F.lit(1)).alias("_rows"),
        F.countDistinct("Prscrbr_NPI").alias("_distinct_npi")]
aggs += [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
result = df.agg(*aggs).collect()[0].asDict()
print(f"\nPass 1 took {time.time() - t0:.1f}s")

rows = result.pop("_rows")
print(f"Rows: {rows:,}")
print(f"Distinct prescribers (NPI): {result.pop('_distinct_npi'):,}")
print("\nNulls per column (count, % of rows):")
for col, n in result.items():
    print(f"  {col:<28} {n:>12,}  {100 * n / rows:6.2f}%")

# Pass 2: what values do the suppression flags actually take?
t0 = time.time()
for flag in ["GE65_Sprsn_Flag", "GE65_Bene_Sprsn_Flag"]:
    df.groupBy(flag).count().orderBy(F.desc("count")).show(truncate=False)
print(f"Pass 2 took {time.time() - t0:.1f}s")

# Quick shape checks on the key columns.
df.select(
    F.min(F.length("Prscrbr_NPI")).alias("npi_min_len"),
    F.max(F.length("Prscrbr_NPI")).alias("npi_max_len"),
    F.countDistinct("Prscrbr_Type").alias("specialties"),
    F.countDistinct("Prscrbr_State_Abrvtn").alias("states"),
    F.countDistinct("Brnd_Name").alias("brands"),
).show()

input("\nSpark UI at http://localhost:4040. Look at Jobs and Stages. Press Enter to stop...")
spark.stop()