import argparse
import os

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, Window, functions as F

KEYS = ["code_module", "code_presentation", "id_student"]


def get_spark():
    builder = SparkSession.builder.appName("learnsight-predict")
    if "PYSPARK_GATEWAY_PORT" not in os.environ:  # plain `python` run on the laptop
        builder = builder.master("local[2]").config("spark.driver.memory", "3g")
    return builder.getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="run date from Airflow (YYYY-MM-DD)")
    parser.add_argument("--week", type=int, default=8)
    parser.add_argument("--model-dir", default="models/rf_w8")
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    feats = spark.read.csv(f"data/features/features_w{args.week}.csv", header=True, inferSchema=True)

    model = PipelineModel.load(args.model_dir)
    scored = model.transform(feats).withColumn(
        "risk_score", F.round(vector_to_array("probability")[1], 4)
    )

    w = Window.orderBy(F.col("risk_score").desc())
    out = (
        scored.withColumn("risk_group", F.ntile(5).over(w))
        .withColumn("model_name", F.lit("RandomForest"))
        .withColumn("scored_on", F.lit(args.date))
        .select(*KEYS, "cutoff_week", "risk_score", "risk_group", "model_name", "scored_on")
    )

    os.makedirs("data/predictions", exist_ok=True)
    out.toPandas().to_csv(f"data/predictions/scores_w{args.week}.csv", index=False)
    print(f"scored {out.count()} enrollments -> data/predictions/scores_w{args.week}.csv")
    out.groupBy("risk_group").agg(F.count("*").alias("n"), F.round(F.avg("risk_score"), 3).alias("avg_score")).orderBy("risk_group").show()