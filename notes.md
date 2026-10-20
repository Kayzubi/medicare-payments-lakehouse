# Write-up notes

- Step 1: Started with a question, not a tool. The NPI is what links two federal datasets that were never designed to be joined.

Spark 4.2.0 on Java 25.0.3, Intel Mac, 4 cores, 6 GB driver memory. Smoke test (10M rows, group-by) ran in 7.5s, mostly startup overhead. Only 4 parallel tasks, so default 200 shuffle partitions will need tuning

- Step 2: Downloaded the 2024 Part D file (SIZE, ROWS lines) with a curl script that resumes on failure and writes a manifest with a SHA-256 checksum. Bronze can be rebuilt with one command, and every result traces back to an exact file version.

- Step 3: Profiled the raw file with every column as a string, so Spark didn’t guess types. One aggregation pass over 3.8 GB of CSV (ROWS rows, PARTITIONS partitions, 4 cores) took X seconds. This is the baseline Parquet has to beat.\


# Decisions log
- 2026-10-19: Profile raw data as strings, not inferSchema. Inference costs an extra full pass and mistypes NPI and FIPS codes. Explicit types go in silver.