import argparse
import os
import sys

from pyspark.sql import SparkSession, functions as F

HERE = os.path.dirname(os.path.abspath(__file__))
SPARK_DIR = os.path.abspath(os.path.join(HERE, ".."))  # spark/ locally, /opt/airflow/jobs in Docker
sys.path.insert(0, os.path.join(HERE, "..", "..", "lakehouse", "jobs"))
from common import GOLD, hudi_options  # noqa: E402

TABLE = "gold_risk_predictions"


def get_spark():
    return (
        SparkSession.builder.appName("publish_predictions")
        .master("local[2]")  # local: the worker cannot see our CSV
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "learnsight"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "learnsight123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="run date from Airflow (YYYY-MM-DD)")
    parser.add_argument("--week", type=int, default=8)
    parser.add_argument("--input", default=None)
    args = parser.parse_args()
    path = args.input or os.path.join(SPARK_DIR, "data", "predictions", f"scores_w{args.week}.csv")

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    scores = (
        spark.read.csv(f"file://{os.path.abspath(path)}", header=True, inferSchema=True)
        .withColumn("scored_on", F.col("scored_on").cast("string"))
        .withColumn("scored_at", F.current_timestamp())
    )

    opts = hudi_options(
        TABLE, "code_module,code_presentation,id_student,cutoff_week", "code_module", "scored_at"
    )
    scores.write.format("hudi").options(**opts).mode("append").save(f"{GOLD}/{TABLE}")

    out = spark.read.format("hudi").load(f"{GOLD}/{TABLE}")
    print(f"[publish_predictions] {TABLE} rows: {out.count()}")