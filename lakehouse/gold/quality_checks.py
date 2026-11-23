"""Airflow step 'quality_checks': final checks on Gold. Exit 1 if a rule fails."""
import argparse, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
for p in (os.path.join(HERE, ".."), os.path.join(HERE, "..", "jobs")):
    sys.path.insert(0, p)
import checks
from common import get_spark, GOLD

ap = argparse.ArgumentParser()
ap.add_argument("--date", default=None)
ap.parse_known_args()

spark = get_spark("quality_checks")
errors = checks.gold_weekly_errors(spark.read.format("hudi").load(f"{GOLD}/gold_weekly_engagement"))
if errors:
    print("QUALITY CHECKS FAILED:")
    for e in errors:
        print(" -", e)
    sys.exit(1)
print("QUALITY CHECKS OK (gold)")
