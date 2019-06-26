#!/usr/bin/env bash

run_backfill () {
    FIRST_RUN_FILE=/usr/local/airflow/.skip_backfill

    if [[ -f "${FIRST_RUN_FILE}" ]]; then
        return
    fi

    echo "=================================================================="
    echo "================== FIRST RUN DETECTED: BACKFILL =================="
    echo "=================================================================="

    airflow unpause 4all_sample_etl
    airflow backfill -s 2018-01-01 -e 2018-01-03 4all_sample_etl

    touch ${FIRST_RUN_FILE}
}

airflow initdb
airflow upgradedb

echo "Running updates"

airflow variables --import /usr/local/airflow/setup/variables.json
airflow pool -s google_maps_api 1 "Google Maps API - Limit to running task to avoid quota errors"
airflow connections -d --conn_id postgres_default
airflow connections -a --conn_id=postgres_default --conn_uri=postgresql://airflow:airflow@postgres/airflow

if [ -z "${TEST}" ]; then
    run_backfill &
    exec /entrypoint.sh webserver
else
    cd "${AIRFLOW_HOME}"
    python3 -m unittest discover -s dags/tests/ -p "test*.py"
fi