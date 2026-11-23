"""Airflow step 'silver_to_gold': builds gold_weekly_engagement from Silver (Hudi)."""
import argparse, os, runpy, sys

HERE = os.path.dirname(os.path.abspath(__file__))
for p in (os.path.join(HERE, ".."), os.path.join(HERE, "..", "jobs")):
    sys.path.insert(0, p)
JOBS = next(p for p in sys.path if os.path.exists(os.path.join(p, "gold_weekly_engagement.py")))

ap = argparse.ArgumentParser()
ap.add_argument("--date", default=None)
ap.parse_known_args()
runpy.run_path(os.path.join(JOBS, "gold_weekly_engagement.py"), run_name="__main__")
print("[silver_to_gold] OK")
