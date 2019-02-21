from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator


dag = DAG ('fdn',
description='fdn_Airflow',
schedule_interval=datetime(days=1),
start_date=datetime(2019, 2, 20), catchup=False
)

t0= DummyOperator(task_id='dummy_task',retries=3, dag=dag)
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag
)
t0>>t1
