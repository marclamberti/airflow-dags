from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy import DummyOperator  # Correct import for Airflow 2.0 and later
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from kubernetes.client import models as k8s

def fetch_dag_data():
    # Replace 'your_conn_id' with your actual Airflow connection ID
    pg_hook = PostgresHook(postgres_conn_id='metadb')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select 'raw_' || mdp_application as source_schema, mdp_table || '_tv' as source_view , 'hist_' || mdp_application as target_schema, mdp_table || '_hist' as target_table from datacontract.mdp_table")
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
    'is_delete_operator_pod': True
}

dag = DAG('DynamicSalesforceLoad',
          default_args=default_args,
          description='Dynamically generated Salesforce load DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2024, 5, 22),
          catchup=False)

# Fetch the data from the database
dag_data = fetch_dag_data()

start_task = DummyOperator(
    task_id='start',
    dag=dag
)

for row in dag_data:
    source_schema, source_view, target_schema, target_table = row
    task_id = f"{target_schema}_{target_table}"
    
    task = KubernetesPodOperator(
        image="antonkuiper/mdpsqlexe",
        name=task_id,
        task_id=task_id,
        arguments=[source_schema, source_view, target_schema, target_table],
        retries=2,
        retry_delay=timedelta(minutes=1),
        dag=dag,
    )
    
    start_task >> task
