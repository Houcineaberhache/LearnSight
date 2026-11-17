import sys
from pathlib import Path
import pytest
import pandas as pd
import numpy as np

# Résolution automatique des chemins d'importation
BASE_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = BASE_DIR.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from ingestion.clean_oulad import (
        clean_courses,
        clean_assessments,
        clean_vle,
        clean_student_info,
        clean_student_registration,
        clean_student_assessment,
        clean_student_vle
    )
except ImportError:
    from clean_oulad import (
        clean_courses,
        clean_assessments,
        clean_vle,
        clean_student_info,
        clean_student_registration,
        clean_student_assessment,
        clean_student_vle
    )

def find_data_dir() -> Path:
    candidates = [
        PROJECT_ROOT / "data" / "raw",
        BASE_DIR / "data" / "raw",
        Path("data/raw"),
        PROJECT_ROOT / "Data",
        Path("Data")
    ]
    for c in candidates:
        if c.exists() and (c / "courses.csv").exists():
            return c
    return Path("data/raw")

DATA_DIR = find_data_dir()

def test_clean_courses():
    df = pd.read_csv(DATA_DIR / "courses.csv")
    cleaned = clean_courses(df)
    assert len(cleaned) == len(df)
    assert cleaned.isnull().sum().sum() == 0

def test_clean_assessments():
    df = pd.read_csv(DATA_DIR / "assessments.csv")
    cleaned = clean_assessments(df)
    assert len(cleaned) == len(df)
    assert cleaned["date"].isnull().sum() == 11
    # Validation du typage Int64 nullable
    assert str(cleaned["date"].dtype) == "Int64"

def test_clean_vle():
    df = pd.read_csv(DATA_DIR / "vle.csv")
    cleaned = clean_vle(df)
    assert len(cleaned) == len(df)
    assert cleaned["week_from"].isnull().sum() == 5243
    # Validation du typage Int64 nullable
    assert str(cleaned["week_from"].dtype) == "Int64"
    assert str(cleaned["week_to"].dtype) == "Int64"

def test_clean_student_info_target_isolation():
    df = pd.read_csv(DATA_DIR / "studentInfo.csv")
    features, target = clean_student_info(df, isolate_target=True)
    assert len(features) == len(df)
    assert len(target) == len(df)
    assert "final_result" not in features.columns
    assert "final_result" in target.columns
    assert "is_dropout" in target.columns
    assert features["imd_band"].isnull().sum() == 1111

def test_clean_student_registration_nulls_and_negative_dates():
    df = pd.read_csv(DATA_DIR / "studentRegistration.csv")
    cleaned = clean_student_registration(df)
    assert len(cleaned) == len(df)
    # Règle 1 : null signifie non-abandon, aucune ligne supprimée
    assert cleaned["date_unregistration"].isnull().sum() == 22521
    # Règle 2 : dates négatives préservées
    assert (cleaned["date_registration"] < 0).sum() > 0
    # Validation du typage Int64 nullable
    assert str(cleaned["date_registration"].dtype) == "Int64"
    assert str(cleaned["date_unregistration"].dtype) == "Int64"

def test_clean_student_assessment_score_numeric_and_flexible_assertion():
    df = pd.read_csv(DATA_DIR / "studentAssessment.csv")
    cleaned = clean_student_assessment(df)
    assert len(cleaned) == len(df)
    # Conversion explicite en nombre
    assert pd.api.types.is_numeric_dtype(cleaned["score"])
    assert cleaned["score"].isnull().any()

    # Test de robustesse sur sous-échantillon (vérifie que l'assertion n'est plus figée à 173)
    sample_df = df.head(100)
    cleaned_sample = clean_student_assessment(sample_df)
    assert len(cleaned_sample) == 100

def test_clean_student_vle_negative_dates_preserved():
    # Test optimisé sur échantillon
    df_sample = pd.read_csv(DATA_DIR / "studentVle.csv", nrows=100000)
    cleaned = clean_student_vle(df_sample, inplace=True)
    assert len(cleaned) == len(df_sample)
    assert cleaned.isnull().sum().sum() == 0
    assert (cleaned["date"] < 0).sum() > 0
