import logging
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.operators import PythonOperator
# Following are defaults which can be overridden later on
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'start_date': datetime(2019, 5, 5),
    'retry_delay': timedelta(minutes=1),
}

def print_click(ds, **kwargs):
    print(" click  dag is called")

dag = DAG('click_dag', default_args=default_args)

activity=PythonOperator(
    task_id='run_click_dag',
    provide_context=True,
    python_callable=print_click,
    dag=dag)
