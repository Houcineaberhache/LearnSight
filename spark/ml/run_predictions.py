import argparse
import os

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, Window, functions as F

KEYS = ["code_module", "code_presentation", "id_student"]


HERE = os.path.dirname(os.path.abspath(__file__))
SPARK_DIR = os.path.abspath(os.path.join(HERE, ".."))  # spark/ locally, /opt/airflow/jobs in Docker


def get_spark():
    return (
        SparkSession.builder.appName("learnsight-predict")
        .master("local[2]")
        .config("spark.driver.memory", "3g")
        .getOrCreate()
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="run date from Airflow (YYYY-MM-DD)")
    parser.add_argument("--week", type=int, default=8)
    parser.add_argument("--model-dir", default=os.path.join(SPARK_DIR, "models", "rf_w8"))
    args = parser.parse_args()
    feats_path = os.path.join(SPARK_DIR, "data", "features", f"features_w{args.week}.csv")
    out_dir = os.path.join(SPARK_DIR, "data", "predictions")
    out_path = os.path.join(out_dir, f"scores_w{args.week}.csv")

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    feats = spark.read.csv(feats_path, header=True, inferSchema=True)

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

    os.makedirs(out_dir, exist_ok=True)
    out.toPandas().to_csv(out_path, index=False)
    print(f"scored {out.count()} enrollments -> {out_path}")