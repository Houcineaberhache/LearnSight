import json
import uuid
import sys
from datetime import datetime, timezone
from pathlib import Path
import jsonschema
import pandas as pd
import pytest

# Résolution automatique des chemins
BASE_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = BASE_DIR.parent

def find_schema_dir() -> Path:
    candidates = [
        BASE_DIR / "schemas",
        PROJECT_ROOT / "ingestion" / "schemas",
        Path("ingestion/schemas"),
        Path("docs/schemas")
    ]
    for c in candidates:
        if c.exists() and (c / "clics_vle_schema.json").exists():
            return c
    return BASE_DIR / "schemas"

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

SCHEMA_DIR = find_schema_dir()
DATA_DIR = find_data_dir()

@pytest.fixture
def vle_schema():
    with open(SCHEMA_DIR / "clics_vle_schema.json", "r", encoding="utf-8") as f:
        return json.load(f)

@pytest.fixture
def eval_schema():
    with open(SCHEMA_DIR / "evaluations_schema.json", "r", encoding="utf-8") as f:
        return json.load(f)

@pytest.fixture
def profile_schema():
    with open(SCHEMA_DIR / "student_profiles_schema.json", "r", encoding="utf-8") as f:
        return json.load(f)

@pytest.fixture
def format_checker():
    return jsonschema.FormatChecker()


# =============================================================================
# 1. Tests Clics VLE
# =============================================================================

def test_vle_schema_valid_sample(vle_schema, format_checker):
    sample = {
        "metadata": {
            "event_id": str(uuid.uuid4()),
            "event_type": "vle_click",
            "produced_at": datetime.now(timezone.utc).isoformat(),
            "source_system": "oulad_simulation_producer",
            "schema_version": "1.0"
        },
        "payload": {
            "code_module": "AAA",
            "code_presentation": "2013J",
            "id_student": 28400,
            "id_site": 546652,
            "date": -10,
            "sum_click": 4
        }
    }
    jsonschema.validate(instance=sample, schema=vle_schema, format_checker=format_checker)


# =============================================================================
# 2. Tests Evaluations (Raw et Enrichi avec assessment_type et weight)
# =============================================================================

def test_eval_schema_raw_with_score(eval_schema, format_checker):
    """Test de l'événement évaluation brut (sans enrichissement)."""
    sample = {
        "metadata": {
            "event_id": str(uuid.uuid4()),
            "event_type": "student_assessment",
            "produced_at": datetime.now(timezone.utc).isoformat(),
            "source_system": "oulad_simulation_producer",
            "schema_version": "1.0"
        },
        "payload": {
            "id_assessment": 1752,
            "id_student": 11391,
            "date_submitted": 18,
            "is_banked": False,
            "score": 78.0
        }
    }
    jsonschema.validate(instance=sample, schema=eval_schema, format_checker=format_checker)

def test_eval_schema_enriched(eval_schema, format_checker):
    """Test de l'événement évaluation enrichi avec assessment_type et weight."""
    sample = {
        "metadata": {
            "event_id": str(uuid.uuid4()),
            "event_type": "student_assessment",
            "produced_at": datetime.now(timezone.utc).isoformat(),
            "source_system": "oulad_simulation_producer",
            "schema_version": "1.0"
        },
        "payload": {
            "id_assessment": 1752,
            "id_student": 11391,
            "date_submitted": 18,
            "is_banked": False,
            "score": 78.0,
            "assessment_type": "TMA",
            "weight": 10.0
        }
    }
    jsonschema.validate(instance=sample, schema=eval_schema, format_checker=format_checker)

def test_eval_schema_with_null_score(eval_schema, format_checker):
    """Gestion des '?' transformés en null pour copies non notées."""
    sample = {
        "metadata": {
            "event_id": str(uuid.uuid4()),
            "event_type": "student_assessment",
            "produced_at": datetime.now(timezone.utc).isoformat(),
            "source_system": "oulad_simulation_producer",
            "schema_version": "1.0"
        },
        "payload": {
            "id_assessment": 1754,
            "id_student": 999999,
            "date_submitted": 120,
            "is_banked": False,
            "score": None,
            "assessment_type": "Exam",
            "weight": 100.0
        }
    }
    jsonschema.validate(instance=sample, schema=eval_schema, format_checker=format_checker)


# =============================================================================
# 3. Tests Profil Étudiant (Correction A: source_system & Anti-Data Leakage)
# =============================================================================

def test_profile_schema_valid(profile_schema, format_checker):
    """Vérifie un profil valide avec source_system obligatoire et SANS final_result."""
    sample = {
        "metadata": {
            "event_id": str(uuid.uuid4()),
            "event_type": "student_profile",
            "produced_at": datetime.now(timezone.utc).isoformat(),
            "source_system": "oulad_simulation_producer",
            "schema_version": "1.0"
        },
        "payload": {
            "code_module": "AAA",
            "code_presentation": "2013J",
            "id_student": 11391,
            "gender": "M",
            "region": "East Anglian Region",
            "highest_education": "HE Qualification",
            "imd_band": "90-100%",
            "age_band": "55<=",
            "num_of_prev_attempts": 0,
            "studied_credits": 240,
            "disability": "N"
        }
    }
    jsonschema.validate(instance=sample, schema=profile_schema, format_checker=format_checker)

def test_profile_schema_rejects_missing_source_system(profile_schema, format_checker):
    """Correction A : source_system est désormais OBLIGATOIRE dans les métadonnées."""
    sample = {
        "metadata": {
            "event_id": str(uuid.uuid4()),
            "event_type": "student_profile",
            "produced_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": "1.0"
            # 'source_system' manquant volontairement
        },
        "payload": {
            "code_module": "AAA",
            "code_presentation": "2013J",
            "id_student": 11391,
            "gender": "M",
            "region": "East Anglian Region",
            "highest_education": "HE Qualification",
            "imd_band": "90-100%",
            "age_band": "55<=",
            "num_of_prev_attempts": 0,
            "studied_credits": 240,
            "disability": "N"
        }
    }
    with pytest.raises(jsonschema.ValidationError) as exc_info:
        jsonschema.validate(instance=sample, schema=profile_schema, format_checker=format_checker)
    assert "'source_system' is a required property" in str(exc_info.value)

def test_profile_schema_rejects_final_result_data_leakage(profile_schema, format_checker):
    """Règle Anti-Leakage : final_result ne doit pas figurer dans le profil d'entrée."""
    sample = {
        "metadata": {
            "event_id": str(uuid.uuid4()),
            "event_type": "student_profile",
            "produced_at": datetime.now(timezone.utc).isoformat(),
            "source_system": "oulad_simulation_producer",
            "schema_version": "1.0"
        },
        "payload": {
            "code_module": "AAA",
            "code_presentation": "2013J",
            "id_student": 11391,
            "gender": "M",
            "region": "East Anglian Region",
            "highest_education": "HE Qualification",
            "imd_band": "90-100%",
            "age_band": "55<=",
            "num_of_prev_attempts": 0,
            "studied_credits": 240,
            "disability": "N",
            "final_result": "Pass"  # Interdit !
        }
    }
    with pytest.raises(jsonschema.ValidationError) as exc_info:
        jsonschema.validate(instance=sample, schema=profile_schema, format_checker=format_checker)
    assert "Additional properties are not allowed ('final_result' was unexpected)" in str(exc_info.value)


# =============================================================================
# 4. Correction B : Validation stricte des formats UUID et date-time
# =============================================================================

def test_format_checker_rejects_invalid_uuid(vle_schema, format_checker):
    """Correction B : Vérifie que FormatChecker bloque les UUIDs invalides."""
    bad_sample = {
        "metadata": {
            "event_id": "this-is-not-a-valid-uuid",
            "event_type": "vle_click",
            "produced_at": datetime.now(timezone.utc).isoformat(),
            "source_system": "oulad_simulation_producer",
            "schema_version": "1.0"
        },
        "payload": {
            "code_module": "AAA",
            "code_presentation": "2013J",
            "id_student": 28400,
            "id_site": 546652,
            "date": -10,
            "sum_click": 4
        }
    }
    with pytest.raises(jsonschema.ValidationError) as exc_info:
        jsonschema.validate(instance=bad_sample, schema=vle_schema, format_checker=format_checker)
    assert "is not a 'uuid'" in str(exc_info.value)

def test_format_checker_rejects_invalid_datetime(vle_schema, format_checker):
    """Correction B : Vérifie que FormatChecker bloque les timestamps invalides."""
    bad_sample = {
        "metadata": {
            "event_id": str(uuid.uuid4()),
            "event_type": "vle_click",
            "produced_at": "invalid-timestamp-2026",
            "source_system": "oulad_simulation_producer",
            "schema_version": "1.0"
        },
        "payload": {
            "code_module": "AAA",
            "code_presentation": "2013J",
            "id_student": 28400,
            "id_site": 546652,
            "date": -10,
            "sum_click": 4
        }
    }
    with pytest.raises(jsonschema.ValidationError) as exc_info:
        jsonschema.validate(instance=bad_sample, schema=vle_schema, format_checker=format_checker)
    assert "is not a 'date-time'" in str(exc_info.value)


# =============================================================================
# 5. Validation sur données réelles OULAD
# =============================================================================

def test_validate_real_data_samples(vle_schema, eval_schema, format_checker):
    # Test avec de vraies lignes de studentVle.csv
    vle_df = pd.read_csv(DATA_DIR / "studentVle.csv", nrows=5)
    for _, row in vle_df.iterrows():
        msg = {
            "metadata": {
                "event_id": str(uuid.uuid4()),
                "event_type": "vle_click",
                "produced_at": datetime.now(timezone.utc).isoformat(),
                "source_system": "oulad_simulation_producer",
                "schema_version": "1.0"
            },
            "payload": {
                "code_module": str(row["code_module"]),
                "code_presentation": str(row["code_presentation"]),
                "id_student": int(row["id_student"]),
                "id_site": int(row["id_site"]),
                "date": int(row["date"]),
                "sum_click": int(row["sum_click"])
            }
        }
        jsonschema.validate(instance=msg, schema=vle_schema, format_checker=format_checker)

    # Test avec de vraies lignes de studentAssessment.csv (enrichies avec assessments.csv)
    assessments_df = pd.read_csv(DATA_DIR / "assessments.csv").set_index("id_assessment")
    eval_df = pd.read_csv(DATA_DIR / "studentAssessment.csv", nrows=5, na_values=["?"])

    for _, row in eval_df.iterrows():
        score_val = float(row["score"]) if pd.notna(row["score"]) else None
        ass_id = int(row["id_assessment"])
        ass_info = assessments_df.loc[ass_id] if ass_id in assessments_df.index else None
        assert ass_info is not None, f"Assessment {ass_id} introuvable"

        msg = {
            "metadata": {
                "event_id": str(uuid.uuid4()),
                "event_type": "student_assessment",
                "produced_at": datetime.now(timezone.utc).isoformat(),
                "source_system": "oulad_simulation_producer",
                "schema_version": "1.0"
            },
            "payload": {
                "id_assessment": ass_id,
                "id_student": int(row["id_student"]),
                "date_submitted": int(row["date_submitted"]),
                "is_banked": bool(int(row["is_banked"])),
                "score": score_val,
                "assessment_type": str(ass_info["assessment_type"]),
                "weight": float(ass_info["weight"])              
            }
        }
        jsonschema.validate(instance=msg, schema=eval_schema, format_checker=format_checker)
