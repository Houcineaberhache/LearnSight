# Lakehouse (Student 2) — MinIO + Spark + Hudi

Kafka -> **Bronze** (raw Parquet) -> **Silver** (Hudi, typed, deduplicated) -> **Gold** (Hudi, aggregates).

| Layer | Table (bucket) | Key | Columns |
|---|---|---|---|
| Bronze | `bronze_clics_vle`, `bronze_evaluations` (`learnsight-bronze`) | kafka topic/partition/offset | raw_value, kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, ingested_at |
| Silver | `silver_clics_vle` (`learnsight-silver`) | event_id | event_id, produced_at, code_module, code_presentation, id_student, id_site, date, sum_click |
| Silver | `silver_evaluations` | id_assessment + id_student | event_id, produced_at, code_module, code_presentation (joined from silver_assessments), id_assessment, id_student, date_submitted, is_banked, score, weight, assessment_type |
| Silver (static) | `silver_assessments` | id_assessment | id_assessment, code_module, code_presentation, assessment_type, date, weight |
| Silver (static) | `silver_student_registration` | module + presentation + student | date_registration, date_unregistration |
| Silver (static) | `silver_student_info` | module + presentation + student | profile columns, **no final_result** |
| Gold | `gold_weekly_engagement` (`learnsight-gold`) | module + presentation + student + week | total_clicks, active_days, clicks_change_vs_prev_week, updated_at (week = floor(date/7)) |

`gold_weekly_engagement` is **sparse**: only weeks with at least one click have a row. `clicks_change_vs_prev_week` compares with the previous *active* week.

Note: `silver_clics_vle` is keyed on `event_id` because several clicks can share student+site+date.

## Run (local, from the project root)
```
.\scripts\run_lakehouse_job.ps1 silver_static.py
.\scripts\run_lakehouse_job.ps1 bronze_ingest.py clics_vle
.\scripts\run_lakehouse_job.ps1 silver.py clics_vle
.\scripts\run_lakehouse_job.ps1 gold_weekly_engagement.py
```
Airflow entry points (accept `--date YYYY-MM-DD`, exit code 0 = success):
`silver/bronze_to_silver.py`, `silver/validate_silver.py`, `gold/silver_to_gold.py`, `gold/quality_checks.py`.

## spark-submit packages required
```
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 --conf spark.jars.ivy=/tmp/.ivy
```
Environment (defaults in `jobs/common.py`): `MINIO_ENDPOINT=http://minio:9000`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`.

## Export to Postgres (for Superset)
`.\scripts\run_lakehouse_job.ps1 export_gold_to_postgres.py` copies the Gold tables to PostgreSQL (`learnsight` database, same table names). Needs the `postgres` service and the `org.postgresql:postgresql:42.7.4` package (already in `run_job.sh`).
