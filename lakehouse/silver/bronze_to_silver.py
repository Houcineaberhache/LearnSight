"""Airflow step 'bronze_to_silver': static tables, then Kafka -> Bronze -> Silver (Hudi), both topics.
Usage: bronze_to_silver.py [--date YYYY-MM-DD]   (exit code 0 = success)
Bronze reads everything new since the last checkpoint, so --date is only logged.
"""
import argparse, os, runpy, sys

HERE = os.path.dirname(os.path.abspath(__file__))
for p in (os.path.join(HERE, ".."), os.path.join(HERE, "..", "jobs")):
    sys.path.insert(0, p)
JOBS = next(p for p in sys.path if os.path.exists(os.path.join(p, "bronze_ingest.py")))

ap = argparse.ArgumentParser()
ap.add_argument("--date", default=None)
args, _ = ap.parse_known_args()
print(f"[bronze_to_silver] run date: {args.date}")

sys.argv = ["silver_static.py"]
print("[bronze_to_silver] silver_static.py")
runpy.run_path(os.path.join(JOBS, "silver_static.py"), run_name="__main__")

for topic in ("clics_vle", "evaluations"):
    for script in ("bronze_ingest.py", "silver.py"):
        sys.argv = [script, topic]
        print(f"[bronze_to_silver] {script} {topic}")
        runpy.run_path(os.path.join(JOBS, script), run_name="__main__")
print("[bronze_to_silver] OK")
