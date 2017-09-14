from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from aarp.common.utils import createCluster
from aarp.adobe.landing import extractTar
from aarp.adobe.lake import startAdobeLakeJob, startUTCJob
from aarp.r4g.r4gingest import import filelanding,checkclusterstatus,jobrun


# adding customised parameters
dag = DAG(
    dag_id='main_dag',
    start_date=datetime(2017,6,15),
    catchup= False,
    schedule_interval='@daily')

adobe1 = PythonOperator(
    task_id='adobe_untar',
    python_callable=extractTar,
    dag=dag
)

adobe2 = PythonOperator(
    task_id='adobe_lake',
    python_callable=startAdobeLakeJob,
    dag=dag
)

adobe3 = PythonOperator(
    task_id='adobe_UTC',
    python_callable=startUTCJob,
    dag=dag
)

r4g_ingest = PythonOperator(
    task_id='r4g_file_landing',
    python_callable=filelanding,
    dag=dag
)

r4g_local = PythonOperator(
    task_id='r4g_local_load',
    python_callable=jobrun('local'),
    dag = dag
)

jobrunjson = {
    "clusterid":r4g2.python_callable,
    "filetype" :CONFIG['auctionfile'],
    "schemaname":CONFIG['auctionschema']
    }

r4g_job2 = PythonOperator(
    task_id='jobrun_task4',
    python_callable=jobrun(jobrunjson),
    dag = dag
)


jobrunjson = {
    "clusterid":r4g2.python_callable,
    "filetype" : CONFIG['merchfile'],
    "schemaname":CONFIG['merchschema']
    }

r4g_job3 = PythonOperator(
    task_id='jobrun_task5',
    python_callable=jobrun(jobrunjson),
    dag = dag
)


jobrunjson = {
    "clusterid":r4g2.python_callable,
    "filetype" : CONFIG['pointfile'],
    "schemaname":CONFIG['pointschema']
    }

r4g_job4 = PythonOperator(
    task_id='jobrun_task6',
    python_callable=jobrun(jobrunjson),
    dag = dag
)

jobrunjson = {
    "clusterid":r4g2.python_callable,
    "filetype" : CONFIG['promofile'],
    "schemaname":CONFIG['promoschema']
    }

r4g_job5 = PythonOperator(
    task_id='jobrun_task7',
    python_callable=jobrun(jobrunjson),
    dag = dag
)


jobrunjson = {
    "clusterid":r4g2.python_callable,
    "filetype" : CONFIG['regfile'],
    "schemaname":CONFIG['regschema']
    }

r4g_job6 = PythonOperator(
    task_id='jobrun_task8',
    python_callable=jobrun(jobrunjson),
    dag = dag
)


jobrunjson = {
    "clusterid":r4g2.python_callable,
    "filetype" : CONFIG['sweepile'],
    "schemaname":CONFIG['sweepschema']
    }

r4g_job7 = PythonOperator(
    task_id='jobrun_task9',
    python_callable=jobrun(jobrunjson),
    dag = dag
)

jobrunjson = {
    "clusterid":r4g2.python_callable,
    "filetype" : CONFIG['travelfile'],
    "schemaname":CONFIG['travelschema']
    }

r4g_job8 = PythonOperator(
    task_id='jobrun_task10',
    python_callable=jobrun(jobrunjson),
    dag = dag
)


adobe2.set_upstream(adobe1)
adobe3.set_upstream(adobe2)

r4g2.set_upstream(r4g1)

r4g_job1.set_upstream(r4g2)
r4g_job2.set_upstream(r4g2)
r4g_job3.set_upstream(r4g2)
r4g_job4.set_upstream(r4g2)
r4g_job5.set_upstream(r4g2)
r4g_job6.set_upstream(r4g2)
r4g_job7.set_upstream(r4g2)
r4g_job8.set_upstream(r4g2)

