%pip install "apache-airflow[celery]==2.5.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.7.txt"

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import time

dag_name = 'CodingChallengeDAG'

default_args = {
    'owner': 'Airflow',
    'start_date':datetime(2022,12,22),
    'schedule_interval':'@once',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': False,
    'catchup':False
}

with DAG(dag_name,default_args=default_args) as dag:

    t1 = DummyOperator(task_id="Task_1")
    t2 = DummyOperator(task_id="Task_2")
    t3 = DummyOperator(task_id="Task_3")
    t4 = DummyOperator(task_id="Task_4")
    t5 = DummyOperator(task_id="Task_5")
    t6 = DummyOperator(task_id="Task_6")
    
    t1 >> [t2, t3] 
    t2 >> [t4, t5, t6]
    t3 >> [t4, t5, t6]
    