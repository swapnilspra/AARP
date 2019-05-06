import logging
from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
import sys
sys.path.append('/usr/local/airflow/dags')
from sensors.rabbitmq_sensor import RabbitMQSensor

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

RABBITMQ_HOST='localhost'
RABBITMQ_USER='airflow'
RABBITMQ_PASS='airflow'
RABBITMQ_VIRTUAL_HOST='/'
NOTIFY_EMAIL="someuser@8451.com"
ENVIRONMENT="dev"

# each Workflow/DAG must have a unique text identifier
WORKFLOW_DAG_ID = 'messaging_sensor'

# start/end times are datetime objects
# here we start execution on Jan 1st, 2017
WORKFLOW_START_DATE = datetime(2019, 5, 5)

# schedule/retry intervals are timedelta objects
# here we execute the DAGs tasks every day
WORKFLOW_SCHEDULE_INTERVAL = timedelta(1)

# default arguments are applied by default to all tasks
# in the DAG
WORKFLOW_DEFAULT_ARGS = {
    'owner': 'mediametrics',
    'depends_on_past': False,
    'start_date': WORKFLOW_START_DATE,
    'email': [NOTIFY_EMAIL],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=10),
    'provide_context': True
}


def auto_confirm_run_dag(context, dag_run_obj):
    return (dag_run_obj)


def recreate_main_dag(dag, env):
    print("Main dag is set to rerun for the next rabbitmq watch", dag.dag_id)
    retrigger_dag = TriggerDagRunOperator(task_id="next_rerun",
                                          trigger_dag_id=WORKFLOW_DAG_ID + "_" + env,
                                          python_callable=auto_confirm_run_dag,
                                          email_on_failure=False,
                                          email=NOTIFY_EMAIL,
                                          dag=dag)
    return retrigger_dag

def decide_execution_path( dag, *args, **kwargs):
    branch = Variable.get('mediametrics.branch.route')
    LOGGER.info('Brancking to : %s', branch)
    return branch



# initialize the DAG
dag = DAG(
    dag_id=WORKFLOW_DAG_ID + "_" + ENVIRONMENT,
    schedule_interval=None,
    default_args=WORKFLOW_DEFAULT_ARGS
)

sensor_task = RabbitMQSensor(conn_id=RABBITMQ_HOST,
                             user=RABBITMQ_USER,
                             password=RABBITMQ_PASS,
                             virtualhost=RABBITMQ_VIRTUAL_HOST,
                             task_id='rabbitmq_sensor',
                             external_dag_id='rabbitmq_sensor_10',
                             poke_interval=30,
                             execution_delta=timedelta(minutes=-5),
                             provide_context=True,
                             dag=dag)

click_dag = TriggerDagRunOperator(task_id="click_dag",
                                          trigger_dag_id='click_dag',
                                          python_callable=auto_confirm_run_dag,
                                          email_on_failure=True,
                                          email=NOTIFY_EMAIL,
                                          dag=dag)

activity_dag = TriggerDagRunOperator(task_id="activity_dag",
                                          trigger_dag_id='activity_dag',
                                          python_callable=auto_confirm_run_dag,
                                          dag=dag)

impression_dag = TriggerDagRunOperator(task_id="impression_dag",
                                          trigger_dag_id='impression_dag',
                                          python_callable=auto_confirm_run_dag,
                                          dag=dag)

branch_task = BranchPythonOperator(task_id='branching',
                                   python_callable=decide_execution_path,
                                   provide_context=True,
                                   dag=dag)

dummy_task = DummyOperator(task_id='dummy_task', dag=dag)
#rerun_listener_task = recreate_main_dag(dag, ENVIRONMENT)

dummy_task >> sensor_task
#sensor_task >> rerun_listener_task
sensor_task >> branch_task
branch_task >> click_dag
branch_task >> activity_dag
branch_task >> impression_dag
