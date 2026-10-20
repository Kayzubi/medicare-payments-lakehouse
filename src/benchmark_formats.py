import time
from pyspark.sql import SparkSession, functions as F

CSV = "data/bronze/partd/partd_prescriber_drug_2024.csv"
PQ = "data/bronze/partd/parquet"

spark = (
    SparkSession.builder
    .appName("format-benchmark")
    .master("local[*]")
    .config("spark.driver.memory", "6g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

def queries(df):
    return {
        "count rows": lambda: df.count(),
        "cost by state": lambda: df.groupBy("Prscrbr_State_Abrvtn")
            .agg(F.sum(F.col("Tot_Drug_Cst").cast("double"))).collect(),
        "distinct NPI": lambda: df.select(F.countDistinct("Prscrbr_NPI")).collect(),
    }

sources = {
    "csv": spark.read.option("header", True).csv(CSV),
    "parquet": spark.read.parquet(PQ),
}

print(f"{'query':<15}{'format':<10}{'run 1':>8}{'run 2':>8}")
for name in ["count rows", "cost by state", "distinct NPI"]:
    for fmt, df in sources.items():
        times = []
        for _ in range(2):
            t0 = time.time()
            queries(df)[name]()
            times.append(time.time() - t0)
        print(f"{name:<15}{fmt:<10}{times[0]:>7.1f}s{times[1]:>7.1f}s")

spark.stop()