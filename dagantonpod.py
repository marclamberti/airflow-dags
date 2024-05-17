from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 16),
    'retries': 7,
    'retry_delay': timedelta(minutes=5),
    # KubernetesPodOperator Defaults
    'namespace': 'airflow-workload',
    'in_cluster': True,  # if set to true, will look in the cluster, if false, looks for file
    'get_logs': True,
    'is_delete_operator_pod': True
}

dag = DAG('mdpsqlexe',
          default_args=default_args,
          description='mdp test voor de sql executor  connectie naar postgres database vanuit een image die gestart wordt vanuit airflow',
          schedule_interval='0 12 * * *',
          start_date=datetime(2024, 5, 15),
          catchup=False)

env_var = [k8s.V1EnvVar(name='FOO', value='foo'), k8s.V1EnvVar(name='BAR', value='bar')]
configmaps = [k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='my-configs'))]

testdb = KubernetesPodOperator(
            image="antonkuiper/mdpsqlexe",
            arguments=["hist_salesforce","Bla2" ,"bla3"],
#            env_vars=env_var,
#            env_from=configmaps,
            name=f"start_sql",
            task_id=f"dbconnectietest",
            retries=2,
            retry_delay=timedelta(minutes=1),
            dag=dag,
        )

load_data = KubernetesPodOperator(
            image="antonkuiper/mdpsqlexe",
#            arguments=["test2"],
            name=f"stop_sqltest",
            task_id=f"nog_een_keer",
            retries=2,
            retry_delay=timedelta(minutes=1),
            dag=dag,
        )


testdb >> load_data