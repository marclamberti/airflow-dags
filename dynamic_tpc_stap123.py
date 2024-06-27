from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from kubernetes.client import models as k8s

# Define the mdp_application value only once
# Dit zou cool zijn als deze nu eens te voorschijnt komt
MDP_APPLICATION = 'tpc'  # Change this value as needed
MDP_SCHEMA = 'public'
p_concurrency=14   # parameter for concurrency

def get_query(mdp_application):
    query = f"""
    SELECT 
		mdp_table AS table_name, 
        mdp_application || '_raw' AS source_schema, 
        mdp_table || '_tv' AS source_view, 
        mdp_application || '_hist' AS target_schema, 
        mdp_table || '_hist' AS target_table 
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
dag = DAG(f'Dynamic_{MDP_APPLICATION}_stap123_load_dwh_from_source',
          default_args=default_args,
          description=f'Dynamically generated {MDP_APPLICATION} pipeline DAG',
          schedule_interval='0 12 * 1 *',
          start_date=datetime(2024, 5, 22),
          concurrency=p_concurrency,
          catchup=False)

# Fetch the data from the database
dag_data = fetch_dag_data(MDP_APPLICATION)

start_task = DummyOperator(
    task_id='start',
    dag=dag
)

for row in dag_data:
    table_name, source_schema, source_view, target_schema, target_table = row
    
    # Unload to S3 Task
    unload_task_id = f"stap1_load_bucket_{table_name}"
    stap1_unload_task = KubernetesPodOperator(
        image="antonkuiper/mdpsqlexe:latest",
        image_pull_policy='Always',
        name=unload_task_id,
        task_id=unload_task_id,
        arguments=["mdpunload2s3.py", MDP_APPLICATION, MDP_SCHEMA, table_name],
        retries=0,
        retry_delay=timedelta(minutes=1),
        dag=dag,
    )
    
    # Load from S3 into Raw Task
    load_raw_task_id = f"stap2_load_raw_{table_name}"
    stap2_load_raw_task = KubernetesPodOperator(
        image="antonkuiper/mdpsqlexe:latest",
        image_pull_policy='Always',
        name=load_raw_task_id,
        task_id=load_raw_task_id,
        arguments=["mdpbucket2raw.py", MDP_APPLICATION, table_name],
        retries=0,
        retry_delay=timedelta(minutes=1),
        dag=dag,
    )
    
    # Load from Raw into Hist Task
    load_hist_task_id = f"stap3_load_hist_{table_name}"
    stap3_load_hist_task = KubernetesPodOperator(
        image="antonkuiper/mdpsqlexe:latest",
        image_pull_policy='Always',
        name=load_hist_task_id,
        task_id=load_hist_task_id,
        arguments=["sqlexe.py", source_schema, source_view, target_schema, target_table],
        retries=0,
        retry_delay=timedelta(minutes=1),
        dag=dag,
    )
    
    start_task >> stap1_unload_task >> stap2_load_raw_task >> stap3_load_hist_task
