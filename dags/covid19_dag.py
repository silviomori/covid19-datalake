from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

from covid19_dag_settings import default_args, emr_settings
from covid19_python_operations import check_data_exists, stop_airflow_containers

from spark_steps import spark_step_one


# Define a DAG
dag = DAG(
    'covid19_dag',
    default_args=default_args,
    max_active_runs=1,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@once',
    is_paused_upon_creation=False)


starting_point = DummyOperator(task_id='starting_point', dag=dag)


check_data_exists_task = PythonOperator(
    task_id='check_data_exists',
    python_callable=check_data_exists,
    op_kwargs={'bucket': 'covid19-lake',
               'prefix': 'archived/tableau-jhu/csv',
               'file': 'COVID-19-Cases.csv'},
    provide_context=False,
    dag=dag)


create_emr_cluster_task = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=emr_settings,
    dag=dag)


add_spark_step_one_task = EmrAddStepsOperator(
    task_id='add_spark_step_one',
    job_flow_id="{{task_instance.xcom_pull(" \
                "      'create_emr_cluster'," \
                "      key='return_value')}}",
    steps=spark_step_one.definition,
    dag=dag)


watch_spark_step_one_task = EmrStepSensor(
    task_id='watch_spark_step_one',
    job_flow_id="{{task_instance.xcom_pull(" \
                "      'create_emr_cluster'," \
                "      key='return_value')}}",
    step_id="{{task_instance.xcom_pull(" \
            "  'add_spark_step_one'," \
            "  key='return_value')[0]}}",
    dag=dag)


terminate_emr_cluster_task = EmrTerminateJobFlowOperator(
    task_id='terminate_emr_cluster',
    job_flow_id="{{task_instance.xcom_pull(" \
                "      'create_emr_cluster'," \
                "      key='return_value')}}",
    trigger_rule="all_done",
    dag=dag)


stop_airflow_containers_task = PythonOperator(
    task_id='stop_airflow_containers',
    python_callable=stop_airflow_containers,
    op_kwargs={'cluster': 'covid19-ecs-cluster'},
    provide_context=False,
    dag=dag)


starting_point >> check_data_exists_task
check_data_exists_task >> create_emr_cluster_task
create_emr_cluster_task >> add_spark_step_one_task
add_spark_step_one_task >> watch_spark_step_one_task
watch_spark_step_one_task >> terminate_emr_cluster_task
terminate_emr_cluster_task >> stop_airflow_containers_task
