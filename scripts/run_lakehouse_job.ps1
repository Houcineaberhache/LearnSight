# Usage: .\scripts\run_lakehouse_job.ps1 bronze_ingest.py clics_vle
#        .\scripts\run_lakehouse_job.ps1 silver/bronze_to_silver.py --date 2026-11-23
param([Parameter(Mandatory=$true)][string]$Job, [Parameter(ValueFromRemainingArguments=$true)][string[]]$JobArgs)
$c = "learnsight-spark-master"
docker cp lakehouse/jobs/. "${c}:/opt/lakehouse/"
docker cp lakehouse/silver/. "${c}:/opt/lakehouse/silver/"
docker cp lakehouse/gold/. "${c}:/opt/lakehouse/gold/"
docker exec -u root $c sed -i 's/\r$//' /opt/lakehouse/run_job.sh
docker exec -u root $c bash /opt/lakehouse/run_job.sh $Job @JobArgs
