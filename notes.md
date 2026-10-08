# Write-up notes

- Step 1: Started with a question, not a tool. The NPI is what links two federal datasets that were never designed to be joined.

Spark 4.2.0 on Java 25.0.3, Intel Mac, 4 cores, 6 GB driver memory. Smoke test (10M rows, group-by) ran in 7.5s, mostly startup overhead. Only 4 parallel tasks, so default 200 shuffle partitions will need tuning