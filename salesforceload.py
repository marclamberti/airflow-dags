from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 22),
    'retries': 7,
    'retry_delay': timedelta(minutes=5),
    # KubernetesPodOperator Defaults
    'namespace': 'airflow-workload',
    'in_cluster': True,  # if set to true, will look in the cluster, if false, looks for file
    'get_logs': True,
    'is_delete_operator_pod': True
}

dag = DAG('SalesforceLoad',
          default_args=default_args,
          description='Salesforce load met test op slqexe  refactored',
          schedule_interval='0 12 * * *',
          start_date=datetime(2024, 5, 22),
          catchup=False)

contact = KubernetesPodOperator(
            image="antonkuiper/mdpsqlexe",
            name=f"salesforce_contact",
            task_id=f"salesforce_contact",
            arguments=["raw_salesforce", "contact_tv", "hist_salesforce", "contact_hist"],
            retries=2,
            retry_delay=timedelta(minutes=1),
            dag=dag,
        )

orders = KubernetesPodOperator(
            image="antonkuiper/mdpsqlexe",
            name=f"salesforce_orders",
            task_id=f"salesforce_orders",
            arguments=["raw_salesforce", "orders_tv", "hist_salesforce", "orders_hist"],
            retries=2,
            retry_delay=timedelta(minutes=1),
            dag=dag,
        )

contact >> orders