import time
from pyspark.sql import SparkSession, functions as F

SRC = "data/bronze/partd/parquet"
DST = "data/silver/partd"

spark = (
    SparkSession.builder
    .appName("partd-silver")
    .master("local[*]")
    .config("spark.driver.memory", "6g")
    .config("spark.sql.shuffle.partitions", "16")   # 4 cores; default 200 is overhead
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# The explicit schema: (bronze column, silver name, type)
COLUMNS = [
    ("Prscrbr_NPI", "npi", "string"),
    ("Prscrbr_Last_Org_Name", "last_org_name", "string"),
    ("Prscrbr_First_Name", "first_name", "string"),
    ("Prscrbr_City", "city", "string"),
    ("Prscrbr_State_Abrvtn", "state", "string"),
    ("Prscrbr_State_FIPS", "state_fips", "string"),
    ("Prscrbr_Type", "specialty", "string"),
    ("Prscrbr_Type_Src", "specialty_source", "string"),
    ("Brnd_Name", "brand_name", "string"),
    ("Gnrc_Name", "generic_name", "string"),
    ("Tot_Clms", "tot_claims", "int"),
    ("Tot_30day_Fills", "tot_30day_fills", "double"),
    ("Tot_Day_Suply", "tot_day_supply", "int"),
    ("Tot_Drug_Cst", "tot_drug_cost", "decimal(14,2)"),
    ("Tot_Benes", "tot_benes", "int"),
    ("GE65_Sprsn_Flag", "ge65_suppression_code", "string"),
    ("GE65_Tot_Clms", "ge65_tot_claims", "int"),
    ("GE65_Tot_30day_Fills", "ge65_tot_30day_fills", "double"),
    ("GE65_Tot_Drug_Cst", "ge65_tot_drug_cost", "decimal(14,2)"),
    ("GE65_Tot_Day_Suply", "ge65_tot_day_supply", "int"),
    ("GE65_Bene_Sprsn_Flag", "ge65_bene_suppression_code", "string"),
    ("GE65_Tot_Benes", "ge65_tot_benes", "int"),
]
NUMERIC = [(src, name) for src, name, t in COLUMNS if t != "string"]

raw = spark.read.parquet(SRC)

# Typed columns, plus raw copies of numeric columns so we can count cast failures.
typed = raw.select(
    *[F.expr(f"try_cast(trim(`{src}`) AS {t})").alias(name) for src, name, t in COLUMNS],
    *[F.trim(F.col(src)).alias(f"_raw_{name}") for src, name in NUMERIC],
    "_source_file", "_ingested_at",
).withColumns({
    "tot_benes_suppressed": F.col("tot_benes").isNull(),
    "ge65_suppressed": F.col("ge65_suppression_code").isNotNull(),
    "ge65_bene_suppressed": F.col("ge65_bene_suppression_code").isNotNull(),
    "npi_valid": F.col("npi").rlike(r"^[0-9]{10}$"),
})

# Quality report: one pass over bronze.
t0 = time.time()
checks = [F.count(F.lit(1)).alias("rows"),
          F.sum((~F.col("npi_valid")).cast("int")).alias("invalid_npi"),
          F.sum((F.col("ge65_suppressed") != F.col("ge65_tot_claims").isNull()).cast("int"))
           .alias("ge65_flag_mismatch"),
          F.sum((F.col("ge65_bene_suppressed") != F.col("ge65_tot_benes").isNull()).cast("int"))
           .alias("ge65_bene_flag_mismatch"),
          F.sum((F.col("tot_claims") < 11).cast("int")).alias("claims_under_11"),
          F.sum((F.col("tot_drug_cost") < 0).cast("int")).alias("negative_cost")]
checks += [F.sum((F.col(f"_raw_{n}").isNotNull() & F.col(n).isNull()).cast("int"))
            .alias(f"cast_fail_{n}") for _, n in NUMERIC]
report = typed.agg(*checks).collect()[0].asDict()
print(f"\nQuality pass took {time.time() - t0:.1f}s")
for k, v in report.items():
    print(f"  {k:<32} {v:>12,}")

# Duplicate keys: same prescriber, brand and generic more than once?
dupes = (typed.groupBy("npi", "brand_name", "generic_name").count()
         .filter("count > 1"))
print(f"\nDuplicate (npi, brand, generic) keys: {dupes.count():,}")
dupes.show(5, truncate=False)

# Which state codes exist, smallest first (finds territories and odd codes).
typed.groupBy("state").count().orderBy("count").show(15)

# Write silver: drop the _raw_ helper columns.
t0 = time.time()
silver = typed.drop(*[f"_raw_{n}" for _, n in NUMERIC])
silver.write.mode("overwrite").parquet(DST)
print(f"Wrote silver in {time.time() - t0:.1f}s")
silver_back = spark.read.parquet(DST)
print(f"Silver rows: {silver_back.count():,}")
silver_back.printSchema()

spark.stop()