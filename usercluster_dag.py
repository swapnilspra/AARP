from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from aarp.common.utils import createCluster


def createUserCluster():
    createCluster(name="userCluster")

startDag = DAG(
    dag_id='start_cluster',
    start_date=datetime(2017,7,15),
    catchup= False,
    schedule_interval='0 11 * * 1-5')

t1 = PythonOperator(
    task_id='start_cluster',
    python_callable=createUserCluster,
    dag=startDag
)