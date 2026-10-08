# Medicare Payments Lakehouse

Do prescribers who receive drug company payments prescribe more of those companies' drugs?
Which prescribers are outliers against peers in the same specialty and state?

Data: CMS Medicare Part D Prescribers by Provider and Drug (2024), CMS Open Payments (2024).
Stack: PySpark, Parquet/Delta, medallion layers (bronze, silver, gold), MLflow, Power BI.
