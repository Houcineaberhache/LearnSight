"""Static reference tables (no Kafka): CSV -> Silver (Hudi).
silver_assessments, silver_student_registration, silver_student_info (final_result never loaded).
CSV folder: STATIC_DIR (default /opt/data/processed).
"""
import os
from pyspark.sql import functions as F
from common import get_spark, hudi_options, SILVER

STATIC_DIR = os.getenv("STATIC_DIR", "/opt/data/processed")

TABLES = {
    "silver_assessments": dict(
        file="assessments.csv", key="id_assessment", ints=["id_assessment", "date"], dbls=["weight"]),
    "silver_student_registration": dict(
        file="studentRegistration.csv", key="code_module,code_presentation,id_student",
        ints=["id_student", "date_registration", "date_unregistration"], dbls=[]),
    "silver_student_info": dict(
        file="studentInfo.csv", key="code_module,code_presentation,id_student",
        ints=["id_student", "num_of_prev_attempts", "studied_credits"], dbls=[]),
}


def load_static(spark, path, ints, dbls):
    df = spark.read.option("header", True).csv(path)
    if "final_result" in df.columns:  # the ML target must never reach features
        df = df.drop("final_result")
    for c in ints:
        df = df.withColumn(c, F.col(c).cast("int"))
    for c in dbls:
        df = df.withColumn(c, F.col(c).cast("double"))
    return df.withColumn("loaded_at", F.current_timestamp())


if __name__ == "__main__":
    spark = get_spark("silver_static")
    for table, cfg in TABLES.items():
        df = load_static(spark, f"file://{STATIC_DIR}/{cfg['file']}", cfg["ints"], cfg["dbls"])
        opts = hudi_options(table, cfg["key"], "code_module", "loaded_at")
        df.write.format("hudi").options(**opts).mode("append").save(f"{SILVER}/{table}")
        print(f"{table}: {spark.read.format('hudi').load(f'{SILVER}/{table}').count()} rows")
