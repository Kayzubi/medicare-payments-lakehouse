# Write-up notes

- Step 1: Started with a question, not a tool. The NPI is what links two federal datasets that were never designed to be joined.

Spark 4.2.0 on Java 25.0.3, Intel Mac, 4 cores, 6 GB driver memory. Smoke test (10M rows, group-by) ran in 7.5s, mostly startup overhead. Only 4 parallel tasks, so default 200 shuffle partitions will need tuning

- Step 2: Downloaded the 2024 Part D file (SIZE, ROWS lines) with a curl script that resumes on failure and writes a manifest with a SHA-256 checksum. Bronze can be rebuilt with one command, and every result traces back to an exact file version.

- Step 3: Profiled 28,023,892 rows (3.8 GB CSV, 31 partitions, 4 cores) as strings. One aggregation pass took 135.6s. Suppression flags reconcile exactly with null counts: 12,463,502 GE65 values hidden by rule, not missing. Tot_Benes is 54% suppressed with no flag, so silver adds one. CMS also drops pairs under 11 claims, so we only see heavy prescribing.

- Step 4: Bronze as Parquet, all columns still strings: 3.8 GB to 674 MB (5.8x). Cost by state 64.5s to 9.1s, distinct NPI 60.3s to 6.5s, count 24.2s to 1.5s (metadata). CSV runs didn’t speed up on repeat, so parsing, not disk, was the bottleneck. Conversion took 196s and pays back after about 4 queries.


# Decisions log
- 2026-10-19: Profile raw data as strings, not inferSchema. Inference costs an extra full pass and mistypes NPI and FIPS codes. Explicit types go in silver.