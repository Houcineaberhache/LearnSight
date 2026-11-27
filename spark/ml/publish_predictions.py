import argparse
import os
import sys

from pyspark.sql import functions as F

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "lakehouse", "jobs"))
from common import GOLD, get_spark, hudi_options 

TABLE = "gold_risk_predictions"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="run date from Airflow (YYYY-MM-DD)")
    parser.add_argument("--week", type=int, default=8)
    parser.add_argument("--input", default=None)
    args = parser.parse_args()
    path = args.input or f"data/predictions/scores_w{args.week}.csv"

    spark = get_spark("publish_predictions")
    spark.sparkContext.setLogLevel("WARN")

    scores = (
        spark.read.csv(path, header=True, inferSchema=True)
        .withColumn("scored_on", F.col("scored_on").cast("string"))
        .withColumn("scored_at", F.current_timestamp())
    )

    opts = hudi_options(
        TABLE, "code_module,code_presentation,id_student,cutoff_week", "code_module", "scored_at"
    )
    scores.write.format("hudi").options(**opts).mode("append").save(f"{GOLD}/{TABLE}")

    out = spark.read.format("hudi").load(f"{GOLD}/{TABLE}")
    print(f"[publish_predictions] {TABLE} rows: {out.count()}")