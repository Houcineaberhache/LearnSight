#!/bin/bash
# Runs inside the spark-master container: run_job.sh <script.py> [args]
set -e
JOB=$1; shift
V=$(spark-submit --version 2>&1 | grep -m1 -oE 'version [0-9]+\.[0-9]+\.[0-9]+' | cut -d' ' -f2)
echo ">>> Spark $V"
cd /opt/lakehouse
spark-submit --master "local[2]" \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:$V,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0" \
  "/opt/lakehouse/$JOB" "$@"
