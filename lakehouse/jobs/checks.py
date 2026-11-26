"""Data-quality rules (pure functions on DataFrames). Each returns a list of error strings."""
from pyspark.sql import functions as F


def _null_counts(df, cols):
    row = df.agg(F.count("*").alias("_n"), *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).collect()[0]
    return row["_n"], {c: (row[c] or 0) for c in cols}


def _generic(df, name, not_null, key):
    errs = []
    n, nulls = _null_counts(df, not_null)
    if n == 0:
        return [f"{name} is empty"]
    errs += [f"{name}: {v} null values in {c}" for c, v in nulls.items() if v]
    dups = df.groupBy(*key).count().filter("count > 1").count()
    if dups:
        errs.append(f"{name}: {dups} duplicated keys on ({', '.join(key)})")
    return errs


def silver_clics_errors(df):
    errs = _generic(df, "silver_clics_vle",
                    ["event_id", "code_module", "code_presentation", "id_student", "id_site", "date", "sum_click"],
                    ["event_id"])
    if df.filter("sum_click < 0").count():
        errs.append("silver_clics_vle: negative sum_click")
    return errs


def silver_evaluations_errors(df):
    errs = _generic(df, "silver_evaluations",
                    ["event_id", "id_assessment", "id_student", "date_submitted", "code_module", "code_presentation"],
                    ["id_assessment", "id_student"])
    if df.filter("score IS NOT NULL AND (score < 0 OR score > 100)").count():
        errs.append("silver_evaluations: score outside 0-100")
    return errs


def gold_weekly_errors(df):
    errs = _generic(df, "gold_weekly_engagement",
                    ["code_module", "code_presentation", "id_student", "week", "total_clicks", "active_days"],
                    ["code_module", "code_presentation", "id_student", "week"])
    if df.filter("total_clicks < 0").count():
        errs.append("gold_weekly_engagement: negative total_clicks")
    if df.filter("active_days < 1 OR active_days > 7").count():
        errs.append("gold_weekly_engagement: active_days outside 1-7")
    return errs


def silver_assessments_errors(df):
    return _generic(df, "silver_assessments", ["id_assessment", "code_module", "code_presentation"], ["id_assessment"])


def silver_registration_errors(df):
    return _generic(df, "silver_student_registration", ["code_module", "code_presentation", "id_student"],
                    ["code_module", "code_presentation", "id_student"])


def silver_student_info_errors(df):
    errs = _generic(df, "silver_student_info", ["code_module", "code_presentation", "id_student"],
                    ["code_module", "code_presentation", "id_student"])
    if "final_result" in df.columns:
        errs.append("silver_student_info: final_result must not be present (target leakage)")
    return errs
