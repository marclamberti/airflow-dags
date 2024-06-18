from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from kubernetes.client import models as k8s

# Define the mdp_application value only once
MDP_APPLICATION = 'tpc'  # Change this value as needed

def get_query(mdp_application):
    query = f"""
    SELECT 
        mdp_application || '' AS source_schema, 
        mdp_table || '' AS target_table 
    FROM datacontract.v_mdp_tables 
    WHERE mdp_application='{mdp_application}'
    """
    return query

def fetch_dag_data(mdp_application):
    pg_hook = PostgresHook(postgres_conn_id='metadb')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = get_query(mdp_application)
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows

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
dag = DAG(f'Dynamic_{MDP_APPLICATION}_Bucket2Raw',
          default_args=default_args,
          description=f'Dynamically generated {MDP_APPLICATION} load from bucket to Raw DAG',
          schedule_interval='0 12 * 1 *',
          start_date=datetime(2024, 5, 22),
          concurrency=7,
          catchup=False)

# Fetch the data from the database
dag_data = fetch_dag_data(MDP_APPLICATION)

start_task = DummyOperator(
    task_id='start',
    dag=dag
)

for row in dag_data:
    source_schema, target_table = row
    task_id = f"{target_table}"
    
    task = KubernetesPodOperator(
        image="antonkuiper/mdpsqlexe:latest",
        image_pull_policy='Always',  # Ensures the latest image is always pulled
        name=task_id,
        task_id=task_id,
        arguments=["mdpBucket2Raw.py", source_schema, target_table ],
        retries=2,
        retry_delay=timedelta(minutes=1),
        dag=dag,
    )
    
    start_task >> task
