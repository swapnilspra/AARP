import logging
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': None,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
def print_activity:
    print(" Activity dag is called")

dag = DAG('activity_dag', default_args=default_args)

activity = TriggerDagRunOperator(task_id="run_activity_dag",
                                          trigger_dag_id='activity_dag',
                                          python_callable=print_activity,
                                          dag=dag)

