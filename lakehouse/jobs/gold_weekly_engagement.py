"""Silver -> Gold: weekly engagement per student/course (week = floor(date / 7))."""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common import get_spark, hudi_options, SILVER, GOLD


def weekly_engagement(silver):
    w = Window.partitionBy("code_module", "code_presentation", "id_student").orderBy("week")
    return (
        silver.withColumn("week", F.floor(F.col("date") / 7).cast("int"))
        .groupBy("code_module", "code_presentation", "id_student", "week")
        .agg(F.sum("sum_click").alias("total_clicks"), F.countDistinct("date").alias("active_days"))
        .withColumn("clicks_change_vs_prev_week", F.col("total_clicks") - F.lag("total_clicks").over(w))
        .withColumn("updated_at", F.current_timestamp())
    )


if __name__ == "__main__":
    spark = get_spark("gold_weekly_engagement")
    silver = spark.read.format("hudi").load(f"{SILVER}/silver_clics_vle")
    gold = weekly_engagement(silver)
    table = "gold_weekly_engagement"
    opts = hudi_options(table, "code_module,code_presentation,id_student,week", "code_module", "updated_at")
    gold.write.format("hudi").options(**opts).mode("append").save(f"{GOLD}/{table}")
    out = spark.read.format("hudi").load(f"{GOLD}/{table}")
    print("GOLD rows:", out.count())
    out.drop(*[c for c in out.columns if c.startswith("_hoodie")]).show(5, truncate=False)
