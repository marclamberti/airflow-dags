# hello.py
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

@dag(
    "hello_world",
    default_args={
        "owner": "Ibinaldo",
        "depends_on_past": False,
        "email": "ibi.best@gmail.com",
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "start_date": datetime(2023,7,23)
    },
    description="DAG to validate git sync",
    schedule_interval="@daily"

)

def hello_world_dag():
    task_start= DummyOperator(task_id="task_start")
    task_end = DummyOperator(task_id="task_end")

    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",

    )

    task_start >> t1 >> task_end

dag = hello_world_dag()