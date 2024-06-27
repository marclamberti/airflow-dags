from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from kubernetes.client import models as k8s

# Define the mdp_application value only once
MDP_APPLICATION = 'tpc'  # Change this value as needed



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 22),
    'retries': 7,
    'retry_delay': timedelta(minutes=5),
    'namespace': 'airflow-workload',
    'in_cluster': True,
    'get_logs': True,
    'is_delete_operator_pod': True,
}

# Use the mdp_application value in the DAG name
dag = DAG(f'Stap0b_{MDP_APPLICATION}_setup_datawarehouse_layers',
          default_args=default_args,
          description=f'mdp system create layers for application {MDP_APPLICATION} ',
          schedule_interval='0 12 * 1 *',
          start_date=datetime(2024, 5, 22),
          concurrency=1,
          catchup=False)

# Fetch the data from the database

start_task = DummyOperator(
    task_id='start',
    dag=dag
)
dag_data= [1]

for row in dag_data:
    task_id = 'Stap0b_setup_datawarehouse_layers_'+ MDP_APPLICATION
    task = KubernetesPodOperator(
        image="antonkuiper/mdpsqlexe:latest",
        image_pull_policy='Always',  # Ensures the latest image is always pulled
        name=task_id,
        task_id=task_id,
        arguments=["mdpcreate.py", MDP_APPLICATION ],
        retries=0,
        retry_delay=timedelta(minutes=1),
        dag=dag,
    )
    
    start_task >> task
