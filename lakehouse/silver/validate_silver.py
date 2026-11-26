"""Airflow step 'validate_silver': data-quality checks on Silver. Exit 1 if a rule fails."""
import argparse, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
for p in (os.path.join(HERE, ".."), os.path.join(HERE, "..", "jobs")):
    sys.path.insert(0, p)
import checks
from common import get_spark, SILVER

ap = argparse.ArgumentParser()
ap.add_argument("--date", default=None)
ap.parse_known_args()

spark = get_spark("validate_silver")
errors = []
errors += checks.silver_clics_errors(spark.read.format("hudi").load(f"{SILVER}/silver_clics_vle"))
errors += checks.silver_evaluations_errors(spark.read.format("hudi").load(f"{SILVER}/silver_evaluations"))
errors += checks.silver_assessments_errors(spark.read.format("hudi").load(f"{SILVER}/silver_assessments"))
errors += checks.silver_registration_errors(spark.read.format("hudi").load(f"{SILVER}/silver_student_registration"))
errors += checks.silver_student_info_errors(spark.read.format("hudi").load(f"{SILVER}/silver_student_info"))
if errors:
    print("VALIDATION FAILED:")
    for e in errors:
        print(" -", e)
    sys.exit(1)
print("VALIDATION OK (silver)")
