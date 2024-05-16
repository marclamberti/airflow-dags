from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries': 7,
    'retry_delay': timedelta(minutes=5),
    # KubernetesPodOperator Defaults
    'namespace': 'airflow-workload',
    'in_cluster': True,  # if set to true, will look in the cluster, if false, looks for file
    'get_logs': True,
    'is_delete_operator_pod': True
}

dag = DAG('kub_sqlexecutor',
          default_args=default_args,
          description='mdp test voor de sql executor  connectie naar postgres database vanuit een image die gestart wordt vanuit airflow',
          schedule_interval='0 12 * * *',
          start_date=datetime(2024, 5, 15),
          catchup=False)

env_var = [k8s.V1EnvVar(name='FOO', value='foo'), k8s.V1EnvVar(name='BAR', value='bar')]
configmaps = [k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='my-configs'))]

ingest_data = KubernetesPodOperator(
            image="antonkuiper/mdppod",
            arguments=["ingest-data"],
#            env_vars=env_var,
#            env_from=configmaps,
            name=f"start_sql",
            task_id=f"sqlexecutor",
            retries=5,
            retry_delay=timedelta(minutes=5),
            dag=dag,
        )

load_data = KubernetesPodOperator(
            image="antonkuiper/mdppod",
            arguments=["nogeenkeer"],
            name=f"sql_executor",
            task_id=f"sql_executor_id",
            retries=5,
            retry_delay=timedelta(minutes=5),
            dag=dag,
        )


ingest_data >> load_data