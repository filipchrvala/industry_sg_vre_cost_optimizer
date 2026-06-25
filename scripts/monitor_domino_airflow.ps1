# Monitor local Domino Airflow runs (docker-compose stack).
# Usage: powershell -File scripts\monitor_domino_airflow.ps1
#        powershell -File scripts\monitor_domino_airflow.ps1 -DagId 8a195dd4d7ec4e29b264a931ffac2201

param(
    [string]$DagId = ""
)

$query = @"
SELECT dag_id, run_id, state, execution_date
FROM dag_run
ORDER BY execution_date DESC
LIMIT 5;
"@

if ($DagId) {
    $query = @"
SELECT task_id, state, try_number, start_date, end_date
FROM task_instance
WHERE dag_id = '$DagId'
ORDER BY start_date NULLS LAST;
"@
}

Write-Host "=== Airflow DAG runs / tasks ===" -ForegroundColor Cyan
docker exec airflow-postgres psql -U airflow -d airflow -c $query
