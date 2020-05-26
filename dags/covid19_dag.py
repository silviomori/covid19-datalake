from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


# Define default_args to be passed in to operators
default_args = {
    'owner': 'root',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define a DAG
dag = DAG(
    'covid19_dag',
    default_args=default_args,
    max_active_runs=1,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@once',
    is_paused_upon_creation=False,
)

starting_point = DummyOperator(task_id='starting_point', dag=dag)
termination = DummyOperator(task_id='termination', dag=dag)

starting_point >> termination