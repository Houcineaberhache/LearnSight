import sys
import os
import argparse
from pyspark.sql import SparkSession, functions as F

KEYS = ["code_module", "code_presentation", "id_student"]


HERE = os.path.dirname(os.path.abspath(__file__))
SPARK_DIR = os.path.abspath(os.path.join(HERE, ".."))  # spark/ locally, /opt/airflow/jobs in Docker


def get_spark():
    return (
        SparkSession.builder.appName("learnsight-features")
        .master("local[2]")  # always local: the worker cannot see our files
        .config("spark.driver.memory", "3g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def load_csv(spark, data_dir, name):
    return spark.read.csv(f"{data_dir}/{name}.csv", header=True, inferSchema=True)

def build_base(spark, data_dir, cutoff_week):
    cutoff_day = 7 * cutoff_week
    reg = load_csv(spark, data_dir, "studentRegistration")
    labels = load_csv(spark, data_dir, "student_target_labels").select(*KEYS, "is_dropout")

    return (
        reg.join(labels, KEYS, "inner")
        .filter(F.col("date_registration").isNull() | (F.col("date_registration") <= cutoff_day))
        .filter(F.col("date_unregistration").isNull() | (F.col("date_unregistration") >= cutoff_day))
        .select(*KEYS, "is_dropout")
        .withColumn("cutoff_week", F.lit(cutoff_week))
    )

def add_vle_features(spark, data_dir, base, cutoff_week):
    cutoff_day = 7 * cutoff_week
    vle = load_csv(spark, data_dir, "studentVle").filter(F.col("date") < cutoff_day)

    agg = vle.groupBy(*KEYS).agg(
        F.sum("sum_click").alias("total_clicks"),
        F.countDistinct("date").alias("active_days"),
        F.sum(F.when(F.col("date") >= cutoff_day - 14, F.col("sum_click")).otherwise(0)).alias("clicks_last_2w"),
    )
    return base.join(agg, KEYS, "left").fillna(0, subset=["total_clicks", "active_days", "clicks_last_2w"])

def add_assessment_features(spark, data_dir, feats, cutoff_week):
    cutoff_day = 7 * cutoff_week
    assess = load_csv(spark, data_dir, "assessments").select(
        "id_assessment", "code_module", "code_presentation", "date"
    )
    sub = load_csv(spark, data_dir, "studentAssessment").join(
        assess.select("id_assessment", "code_module", "code_presentation"), "id_assessment"
    )
    done = sub.filter((F.col("is_banked") == 1) | (F.col("date_submitted") < cutoff_day))
    real = sub.filter((F.col("is_banked") == 0) & (F.col("date_submitted") < cutoff_day))

    due = assess.filter(F.col("date").isNotNull() & (F.col("date") < cutoff_day)).select(
        "id_assessment", "code_module", "code_presentation"
    )
    expected = feats.select(*KEYS).join(due, ["code_module", "code_presentation"])
    done_flag = done.select(*KEYS, "id_assessment").withColumn("submitted", F.lit(1))

    missed = (
        expected.join(done_flag, KEYS + ["id_assessment"], "left")
        .groupBy(*KEYS)
        .agg(F.sum(F.when(F.col("submitted").isNull(), 1).otherwise(0)).alias("n_missed"))
    )
    scores = real.groupBy(*KEYS).agg(
        F.count("*").alias("n_submitted"), F.round(F.avg("score"), 2).alias("avg_score")
    )
    return (
        feats.join(missed, KEYS, "left")
        .join(scores, KEYS, "left")
        .fillna(0, subset=["n_missed", "n_submitted"])
    )

def add_trend_features(spark, data_dir, feats, cutoff_week):
    cutoff_day = 7 * cutoff_week
    vle = load_csv(spark, data_dir, "studentVle").filter(
        (F.col("date") >= 0) & (F.col("date") < cutoff_day)
    )
    weekly = (
        vle.withColumn("week", F.floor(F.col("date") / 7))
        .groupBy(*KEYS, "week")
        .agg(F.sum("sum_click").alias("wk_clicks"))
    )
    agg = weekly.groupBy(*KEYS).agg(
        F.count("*").alias("weeks_active"),
        F.sum(F.when(F.col("week") >= cutoff_week - 2, F.col("wk_clicks")).otherwise(0)).alias("recent"),
        F.sum(F.when(F.col("week") < cutoff_week - 2, F.col("wk_clicks")).otherwise(0)).alias("earlier"),
    )
    out = feats.join(agg, KEYS, "left").fillna(0, subset=["weeks_active", "recent", "earlier"])
    return (
        out.withColumn("weeks_inactive", F.lit(cutoff_week) - F.col("weeks_active"))
        .withColumn("clicks_trend", F.round(F.col("recent") / 2 - F.col("earlier") / (cutoff_week - 2), 2))
        .drop("weeks_active", "recent", "earlier")
    )

def add_info_features(spark, data_dir, feats):
    info = load_csv(spark, data_dir, "studentInfo").select(
        *KEYS, "num_of_prev_attempts", "studied_credits"
    )
    return feats.join(info, KEYS, "left")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="run date from Airflow (YYYY-MM-DD), not used yet")
    parser.add_argument("--week", type=int, default=8)
    parser.add_argument("--data-dir", default=os.getenv("STATIC_DIR", os.path.join(SPARK_DIR, "..", "data", "processed")))
    parser.add_argument("--out-dir", default=os.path.join(SPARK_DIR, "data", "features"))
    args = parser.parse_args()
    data_dir, week = args.data_dir, args.week
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    base = build_base(spark, data_dir, week)
    feats = add_vle_features(spark, data_dir, base, week)
    feats = add_assessment_features(spark, data_dir, feats, week)
    feats = add_trend_features(spark, data_dir, feats, week)
    feats = add_info_features(spark, data_dir, feats).cache()
    feats.show(5)
    feats.groupBy("is_dropout").agg(
        F.count("*").alias("n"),
        F.round(F.avg("num_of_prev_attempts"), 2).alias("avg_prev_attempts"),
        F.round(F.avg("studied_credits"), 1).alias("avg_credits"),
    ).show()

    os.makedirs(args.out_dir, exist_ok=True)
    out_path = os.path.join(args.out_dir, f"features_w{week}.csv")
    feats.toPandas().to_csv(out_path, index=False)
    print(f"saved: {out_path}")