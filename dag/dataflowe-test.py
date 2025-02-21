from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from kubernetes.client import models as k8s

# Define the mdp_application value only once
DFE_APPLICATION = 'adventureworks'  # Change this value as needed
p_concurrency=8   # parameter for concurrency


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 22),
    'retries': 7,
    'retry_delay': timedelta(minutes=5),
    'namespace': 'dbtrunner',
    'in_cluster': True,
    'get_logs': True,
    'is_delete_operator_pod': True,
}

# Use the mdp_application value in the DAG name
dag = DAG(f'Dynamic_{DFE_APPLICATION}_test_dbtrunner',
          default_args=default_args,
          description=f' {DFE_APPLICATION} run dbtrunner in dbtrunner namespace',
          schedule_interval='0 12 * 1 *',
          start_date=datetime(2025, 3, 21),
          concurrency=p_concurrency,
          catchup=False)


start_task = DummyOperator(
    task_id='start',
    dag=dag
)

dbtrunner = KubernetesPodOperator(
    image="antonkuiper/dbt:latest",
    image_pull_policy='Always',  # Ensures the latest image is always pulled
    name='dit is de naam van dbtrunner',
    task_id='dbtrunner-test',
    arguments=["dbtrunner.sh"],
    retries=0,
    retry_delay=timedelta(minutes=1),
    dag=dag,
)

    start_task >> dbtrunner
