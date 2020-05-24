# Standard library import
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator
from airflow.operators import LoadFactOperator
from airflow.operators import LoadDimensionOperator
from airflow.operators import DataQualityOperator
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# Define default_args that will be passed on to each operator
default_args = {
    'owner': 'Udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'end_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False
}

# Define a DAG and use the default_args
dag = DAG(
    'data_pipeline_dag',
    default_args=default_args,
    max_active_runs=5,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)

# Set the DAG begin execution
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# Create all the tables on Redshift
create_tables = PostgresOperator(
    task_id="create_tables",
    postgres_conn_id="redshift",
    sql='create_tables.sql',
    dag=dag,
)

# Stage events data from S3 to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path='s3://udacity-dend/log_json_path.json',
    dag=dag
)

# Stage songs data from S3 to Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_path="auto",
    dag=dag
)

# Load the fact with a custom operator
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    table='songplays',
    redshift_conn_id='redshift',
    select_sql=SqlQueries.songplay_table_insert,
    dag=dag
)

# Load the song dimension with a custom operator
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    table='songs',
    redshift_conn_id='redshift',
    select_sql=SqlQueries.song_table_insert,
    dag=dag
)

# Load the artist dimension with a custom operator
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    table='artists',
    redshift_conn_id='redshift',
    select_sql=SqlQueries.artist_table_insert,
    dag=dag
)

# Load the user dimension with a custom operator
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    table='users',
    redshift_conn_id='redshift',
    select_sql=SqlQueries.user_table_insert,
    dag=dag
)

# Load the time dimension with a custom operator
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    table='time',
    redshift_conn_id='redshift',
    select_sql=SqlQueries.time_table_insert,
    dag=dag
)

# Run the data quality operator
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    tables=['users', 'songs', 'artists', 'time', 'songplays'],
    dag=dag
)

# Set the DAG the end execution
end_operator = DummyOperator(task_id='End_execution',  dag=dag)

# Set the correct dependecies
start_operator >> create_tables
create_tables >>  [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table 
load_songplays_table >> [load_song_dimension_table, load_artist_dimension_table, 
                         load_user_dimension_table, load_time_dimension_table]
[load_song_dimension_table, load_artist_dimension_table, 
 load_user_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator