import os
SPARK_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import pandas as pd
from pyspark.ml.functions import vector_to_array
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import Imputer, VectorAssembler
from pyspark.sql import SparkSession, functions as F

LABEL = "is_dropout"
FEATURES = ["total_clicks", "active_days", "clicks_last_2w", "n_missed", "n_submitted",
            "avg_score", "weeks_inactive", "clicks_trend",
            "num_of_prev_attempts", "studied_credits"]
KEYS = ["code_module", "code_presentation", "id_student"]

def get_spark():
    return (
        SparkSession.builder.appName("learnsight-train")
        .master("local[2]")
        .config("spark.driver.memory", "3g")
        .getOrCreate()
    )


if __name__ == "__main__":
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.csv(os.path.join(SPARK_DIR, "data", "features", "features_w8.csv"), header=True, inferSchema=True)
    df = df.withColumn(LABEL, F.col(LABEL).cast("double"))
    week = int(df.first()["cutoff_week"])
    metrics_rows = []

    train = df.filter(F.col("code_presentation") != "2014J")
    test = df.filter(F.col("code_presentation") == "2014J")

    pos = train.filter(F.col(LABEL) == 1).count()
    ratio = (train.count() - pos) / pos
    train = train.withColumn("w", F.when(F.col(LABEL) == 1, ratio).otherwise(1.0))

    imputer = Imputer(strategy="median", inputCols=["avg_score"], outputCols=["avg_score_f"])
    cols = [c for c in FEATURES if c != "avg_score"] + ["avg_score_f"]
    assembler = VectorAssembler(inputCols=cols, outputCol="features")

    models = {
        "LogisticRegression": LogisticRegression(
            featuresCol="features", labelCol=LABEL, weightCol="w", maxIter=100
        ),
        "RandomForest": RandomForestClassifier(
            featuresCol="features", labelCol=LABEL, weightCol="w",
            numTrees=100, maxDepth=6, seed=42,
        ),
    }

    for name, clf in models.items():
        fitted = Pipeline(stages=[imputer, assembler, clf]).fit(train)
        pred = fitted.transform(test)
        aucs = {}
        for metric in ["areaUnderPR", "areaUnderROC"]:
            ev = BinaryClassificationEvaluator(
                labelCol=LABEL, rawPredictionCol="rawPrediction", metricName=metric
            )
            aucs[metric] = ev.evaluate(pred)
            print(f"{name} | {metric}: {aucs[metric]:.3f}")

        rows = pred.groupBy(LABEL, "prediction").count().collect()
        cm = {(r[LABEL], r["prediction"]): r["count"] for r in rows}
        tp, fp = cm.get((1.0, 1.0), 0), cm.get((0.0, 1.0), 0)
        fn, tn = cm.get((1.0, 0.0), 0), cm.get((0.0, 0.0), 0)
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        print(f"{name} | TP={tp} FP={fp} FN={fn} TN={tn}")
        print(f"{name} | precision={precision:.3f} recall={recall:.3f} f1={f1:.3f}")
        metrics_rows.append({
            "model_name": name, "cutoff_week": week, "test_period": "2014J",
            "pr_auc": round(aucs["areaUnderPR"], 4), "roc_auc": round(aucs["areaUnderROC"], 4),
            "precision": round(precision, 4), "recall": round(recall, 4), "f1": round(f1, 4),
            "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        })       
        if name == "RandomForest":
            scored = pred.withColumn("risk_score", F.round(vector_to_array("probability")[1], 4))
            sp = scored.select(*KEYS, "cutoff_week", "risk_score", LABEL).toPandas()
            sp["risk_group"] = 5 - pd.qcut(sp["risk_score"].rank(method="first"), 5, labels=False)
            print(sp.groupby("risk_group")[LABEL].agg(["count", "mean"]).round(3))
            os.makedirs(os.path.join(SPARK_DIR, "data", "predictions"), exist_ok=True)
            sp.to_csv(os.path.join(SPARK_DIR, "data", "predictions", "predictions_w8.csv"), index=False)
            fitted.write().overwrite().save(os.path.join(SPARK_DIR, "models", "rf_w8"))
            print("model saved: models/rf_w8")

    pd.DataFrame(metrics_rows).to_csv(os.path.join(SPARK_DIR, "data", "predictions", f"model_metrics_w{week}.csv"), index=False)
    print(f"metrics saved: data/predictions/model_metrics_w{week}.csv")