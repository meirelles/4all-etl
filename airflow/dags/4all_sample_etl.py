import os
from datetime import datetime, timedelta
from airflow.models import DAG
from operators.extract import GetDataset
from operators.save_to_postgres import LoadToDatabase
from operators.resolve_addresses import ResolveAddresses


default_args = {
    'owner': 'meirelles',
    'depends_on_past': False,
    'start_date': datetime(2015, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

"""
    Is possible change the schedule interval from @daily, but is required update
    the {{ ds }} macro accordingly.
"""

with DAG(dag_id='4all_sample_etl',
         default_args=default_args,
         catchup=False,
         schedule_interval='@daily',
         dagrun_timeout=timedelta(minutes=30)) as dag:

    get_task = GetDataset(
        task_id='get_dataset',
        input_dir=os.path.realpath(os.path.dirname(__file__) + '/../sample-dataset'),
        input_mask="*{{ ds_nodash }}.txt",
        output_ns="geodir_{{ ds }}",
        dag=dag)

    """
    To avoid some backoff pressure from Google Maps, we use pool= here to limit how many 
    task instances are allowed to run at the same time. 
    """

    resolve_addresses = ResolveAddresses(
        task_id='resolve_addresses',
        input_ns="geodir_{{ ds }}",
        output_ns="geodir_{{ ds }}_resolved",
        pool="google_maps_api",
        dag=dag
    )

    load_to_database = LoadToDatabase(
        task_id='load_to_database',
        input_ns="geodir_{{ ds }}_resolved",
        output_table="sample_output",
        output_ds="{{ ds }}"
    )

    get_task >> resolve_addresses >> load_to_database
