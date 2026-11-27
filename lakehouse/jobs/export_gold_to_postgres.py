"""Gold (Hudi on MinIO) -> PostgreSQL, so Superset can read it (Superset cannot read Hudi).
Copies every Gold table that exists; the table is replaced on each run (idempotent).
Env: POSTGRES_HOST (postgres), POSTGRES_PORT (5432), POSTGRES_DB (learnsight), POSTGRES_USER, POSTGRES_PASSWORD
"""
import os
from common import get_spark, GOLD

GOLD_TABLES = ["gold_weekly_engagement", "gold_student_features", "gold_risk_predictions"]

host = os.getenv("POSTGRES_HOST", "postgres")
port = os.getenv("POSTGRES_PORT", "5432")
db = os.getenv("POSTGRES_DB", "learnsight")
props = {
    "user": os.getenv("POSTGRES_USER", "learnsight"),
    "password": os.getenv("POSTGRES_PASSWORD", "learnsight123"),
    "driver": "org.postgresql.Driver",
    "truncate": "true",  # keep the table (and Superset's dataset), replace its rows
}
url = f"jdbc:postgresql://{host}:{port}/{db}"

spark = get_spark("export_gold_to_postgres")
exported = []
for table in GOLD_TABLES:
    try:
        df = spark.read.format("hudi").load(f"{GOLD}/{table}")
    except Exception as exc:  # table not produced yet (e.g. Abdo's tables)
        print(f"[export] skip {table}: {type(exc).__name__}: {str(exc)[:200]}")
        continue
    df = df.drop(*[c for c in df.columns if c.startswith("_hoodie")])
    df.write.jdbc(url, table, mode="overwrite", properties=props)
    n = spark.read.jdbc(url, table, properties=props).count()
    print(f"[export] {table}: {n} rows -> postgres {db}.{table}")
    exported.append(table)

if not exported:
    raise SystemExit("[export] nothing exported: no Gold table found")
print("[export] OK")
