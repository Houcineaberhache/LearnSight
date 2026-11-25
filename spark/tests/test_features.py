import os
import sys

import pandas as pd
import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "features"))
import compute_features as cf

CUTOFF_WEEK = 2  # cutoff day = 14
KEY = {"code_module": "AAA", "code_presentation": "2013J"}


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("learnsight-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def data_dir(tmp_path_factory):
    d = tmp_path_factory.mktemp("oulad")
    reg = pd.DataFrame([
        {**KEY, "id_student": 1, "date_registration": -5, "date_unregistration": None},
        {**KEY, "id_student": 2, "date_registration": -5, "date_unregistration": 10},
        {**KEY, "id_student": 3, "date_registration": -5, "date_unregistration": 20},
        {**KEY, "id_student": 4, "date_registration": 20, "date_unregistration": None},
    ])
    reg["date_unregistration"] = reg["date_unregistration"].astype("Int64")
    reg.to_csv(d / "studentRegistration.csv", index=False)

    labels = pd.DataFrame([
        {**KEY, "id_student": 1, "final_result": "Pass", "is_dropout": 0},
        {**KEY, "id_student": 2, "final_result": "Withdrawn", "is_dropout": 1},
        {**KEY, "id_student": 3, "final_result": "Withdrawn", "is_dropout": 1},
        {**KEY, "id_student": 4, "final_result": "Pass", "is_dropout": 0},
    ])
    labels.to_csv(d / "student_target_labels.csv", index=False)

    vle = pd.DataFrame([
        {**KEY, "id_student": 1, "id_site": 1, "date": 0, "sum_click": 10},
        {**KEY, "id_student": 1, "id_site": 1, "date": 13, "sum_click": 5},
        {**KEY, "id_student": 1, "id_site": 1, "date": 14, "sum_click": 1000},
        {**KEY, "id_student": 1, "id_site": 1, "date": 30, "sum_click": 1000},
    ])
    vle.to_csv(d / "studentVle.csv", index=False)
    return d.as_posix()


def student_ids(df):
    return {r["id_student"] for r in df.select("id_student").collect()}


def test_base_keeps_only_students_still_registered_at_cutoff(spark, data_dir):
    base = cf.build_base(spark, data_dir, CUTOFF_WEEK)
    assert student_ids(base) == {1, 3}


def test_clicks_ignore_cutoff_day_and_later(spark, data_dir):
    base = cf.build_base(spark, data_dir, CUTOFF_WEEK)
    feats = cf.add_vle_features(spark, data_dir, base, CUTOFF_WEEK)
    row = feats.filter("id_student = 1").first()
    assert row["total_clicks"] == 15
    assert row["active_days"] == 2


def test_student_without_clicks_is_kept_with_zero(spark, data_dir):
    base = cf.build_base(spark, data_dir, CUTOFF_WEEK)
    feats = cf.add_vle_features(spark, data_dir, base, CUTOFF_WEEK)
    row = feats.filter("id_student = 3").first()
    assert row["total_clicks"] == 0
    assert row["active_days"] == 0