"""Bronze -> Silver (Hudi upsert). Parse JSON, type columns, dedupe on the natural key.
Usage: silver.py <clics_vle|evaluations>
"""
import sys
from pyspark.sql import functions as F, types as T
from common import get_spark, hudi_options, BRONZE, SILVER

META = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("produced_at", T.StringType()),
])

TABLES = {
    "clics_vle": dict(
        payload=T.StructType([
            T.StructField("code_module", T.StringType()),
            T.StructField("code_presentation", T.StringType()),
            T.StructField("id_student", T.IntegerType()),
            T.StructField("id_site", T.IntegerType()),
            T.StructField("date", T.IntegerType()),
            T.StructField("sum_click", T.IntegerType()),
        ]),
        key="event_id",
        partition="code_module",
    ),
    "evaluations": dict(
        payload=T.StructType([
            T.StructField("id_assessment", T.IntegerType()),
            T.StructField("id_student", T.IntegerType()),
            T.StructField("date_submitted", T.IntegerType()),
            T.StructField("is_banked", T.BooleanType()),
            T.StructField("score", T.DoubleType()),
            T.StructField("assessment_type", T.StringType()),
            T.StructField("weight", T.DoubleType()),
        ]),
        key="id_assessment,id_student",
        partition="assessment_type",
    ),
}


def parse_bronze(raw, payload_schema):
    schema = T.StructType([
        T.StructField("metadata", META),
        T.StructField("payload", payload_schema),
    ])
    j = raw.withColumn("j", F.from_json("raw_value", schema))
    return (
        j.filter("j.payload IS NOT NULL AND j.metadata.event_id IS NOT NULL")
        .select(
            F.col("j.metadata.event_id").alias("event_id"),
            F.coalesce(F.to_timestamp("j.metadata.produced_at"), F.col("kafka_timestamp")).alias("produced_at"),
            "j.payload.*",
        )
    )


def add_course(evals, assessments):
    """Evaluation events carry only id_assessment; take the course from the static assessments table."""
    a = assessments.select("id_assessment", "code_module", "code_presentation")
    return evals.join(a, "id_assessment", "left")


if __name__ == "__main__":
    topic = sys.argv[1]
    cfg = TABLES[topic]
    spark = get_spark(f"silver_{topic}")
    table = f"silver_{topic}"

    raw = spark.read.parquet(f"{BRONZE}/bronze_{topic}")
    silver = parse_bronze(raw, cfg["payload"])
    if topic == "evaluations":  # needs silver_static.py to have run first
        silver = add_course(silver, spark.read.format("hudi").load(f"{SILVER}/silver_assessments"))

    opts = hudi_options(table, cfg["key"], cfg["partition"], "produced_at")
    silver.write.format("hudi").options(**opts).mode("append").save(f"{SILVER}/{table}")

    out = spark.read.format("hudi").load(f"{SILVER}/{table}")
    print(f"BRONZE rows: {raw.count()} | SILVER rows (after dedup): {out.count()}")
    out.drop(*[c for c in out.columns if c.startswith("_hoodie")]).show(5, truncate=False)
