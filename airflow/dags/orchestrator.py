import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.append('/opt/airflow/youtube')
sys.path.append('/opt/airflow/mwts')

from insert_records import main as main_insert
from get_records_send_video import main as main_video
from get_records_send_channel import main as main_channel
from analytics import main as main_analyt


def safe_call():
    return main_insert()

def safe_call_1():
    return main_video()

def safe_call_2():
    return main_channel()

def safe_call_3():
    return main_analyt()

default_args={
    'description':'DAG to orchestarte data',
    'start_date': datetime(2025, 11, 29),
    'catchup':False,
}

dag = DAG(
    dag_id = 'youtube-api-orchestrator',
    default_args = default_args,
    schedule=timedelta(minutes=40)
)

with dag:
    task1 = PythonOperator(
        task_id = 'ingest_data_task',
        python_callable = safe_call
    )
    task2 = PythonOperator(
        task_id = 'send_data_task_video',
        python_callable = safe_call_1
    )
    task3 = PythonOperator(
        task_id = 'send_data_task_channel',
        python_callable = safe_call_2
    )
    task1 >> task2 >> task3

dag1 = DAG(
    dag_id = 'analytics-orchestrator',
    default_args = default_args,
    schedule=timedelta(minutes=40)
)

with dag1:
    task1 = PythonOperator(
        task_id = 'analytics_send',
        python_callable = safe_call_3
    )