from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from aarp.adobe.landing import extractTar

dag = DAG(dag_id='adobe_landing_dag',
    start_date=datetime(2017,8,15),
    catchup= False,
    max_active_runs=1,
    schedule_interval='@hourly')

t1 = PythonOperator(
    task_id='adobe_untar',
    python_callable=extractTar,
    dag=dag
)
