# Usage: .\scripts\run_lakehouse_job.ps1 bronze_ingest.py clics_vle
param([Parameter(Mandatory=$true)][string]$Job, [Parameter(ValueFromRemainingArguments=$true)][string[]]$JobArgs)
$c = "learnsight-spark-master"
docker cp lakehouse/jobs/. "${c}:/opt/lakehouse/"
docker exec -u root $c sed -i 's/\r$//' /opt/lakehouse/run_job.sh
docker exec -u root $c bash /opt/lakehouse/run_job.sh $Job @JobArgs
