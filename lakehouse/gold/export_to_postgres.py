"""Airflow step 'export_to_postgres': copies the Gold tables to PostgreSQL for Superset."""
import argparse, os, runpy, sys

HERE = os.path.dirname(os.path.abspath(__file__))
for p in (os.path.join(HERE, ".."), os.path.join(HERE, "..", "jobs")):
    sys.path.insert(0, p)
JOBS = next(p for p in sys.path if os.path.exists(os.path.join(p, "export_gold_to_postgres.py")))

ap = argparse.ArgumentParser()
ap.add_argument("--date", default=None)
ap.parse_known_args()
runpy.run_path(os.path.join(JOBS, "export_gold_to_postgres.py"), run_name="__main__")
