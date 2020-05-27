from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator

from covid19_dag_settings import default_args, emr_settings
from operators.termination_operator import TerminationOperator


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

create_emr_cluster_task = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=emr_settings,
    dag=dag
)

termination_task = TerminationOperator(task_id='termination', dag=dag)

starting_point >> create_emr_cluster_task
create_emr_cluster_task >> termination_task
