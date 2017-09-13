from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from aarp.common.utils import destroyCluster


def shutdownUserCluster():
    destroyCluster(name='userCluster')



stopDag = DAG(
    dag_id='stop_cluster',
    start_date=datetime(2017,7,15),
    catchup= False,
    schedule_interval='0 22 * * 1-5')

t2 = PythonOperator(
    task_id='stop_cluster',
    python_callable=shutdownUserCluster,
    dag=stopDag
)