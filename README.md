An exploratory local-first batch data engineering project that uses Dagster and PySpark to transform NYC taxi trip data into easy-access analytics outputs in PostgreSQL.

# Problem statement for the pipeline: 

Which NYC taxi pickup zones and hours generate the highest reliable trip volume and revenue, after filtering invalid trip records?

The pipeline will:

1. Read raw NYC taxi trip data.
2. Materialize bronze Parquet assets with ingestion metadata.
3. Clean and validate trips into silver Parquet assets.
4. Aggregate trusted trips into gold analytics metrics.
5. Load final gold metrics into Postgres.
6. Expose outputs for inspection through pgAdmin.
7. Track basic run and data quality metadata.

## Stack

- Python
- PySpark
- Dagster
- Postgres
- pgAdmin
- Parquet
- pytest
- ruff
- Docker Compose

## End Goal Output

data/raw      -> source files
data/bronze   -> raw-ish normalized Parquet
data/silver   -> cleaned and validated trip-level Parquet
data/gold     -> analytics-ready outputs
Postgres      -> final mart and audit tables