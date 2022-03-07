from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'Dolu',
    'start_date': datetime(2019, 1, 12),
    'Depends_on_past' : False,
    'Retries' : 3,
    'Retry_delay' : timedelta(minutes=5),
    'Catchup' : False 
}

dag = DAG('test_dag4',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval="@hourly"
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_arg="auto ignorecase"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_arg="auto ignorecase"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    insert_statements=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    insert_statements=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    insert_statements=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    insert_statements=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    insert_statements=SqlQueries.time_table_insert
)

time_table_quality_checks = DataQualityOperator(
    task_id='Run_time_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table="time"  
)

users_quality_checks = DataQualityOperator(
    task_id='Run_user_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table="users"  
)

songs_quality_checks = DataQualityOperator(
    task_id='Run_songs_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs"
)


artists_quality_checks = DataQualityOperator(
    task_id='Run_artist_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists"  
)


songplays_quality_checks = DataQualityOperator(
    task_id='Run_songplay_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays"  
)



end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Define task dependency
start_operator                >>    stage_events_to_redshift
start_operator                >>    stage_songs_to_redshift
stage_songs_to_redshift       >>    load_songplays_table
stage_events_to_redshift      >>    load_songplays_table
load_songplays_table          >>    songplays_quality_checks
songplays_quality_checks      >>    load_time_dimension_table
songplays_quality_checks      >>    load_artist_dimension_table
songplays_quality_checks      >>    load_song_dimension_table
songplays_quality_checks      >>    load_user_dimension_table
load_user_dimension_table     >>    users_quality_checks
load_artist_dimension_table   >>    artists_quality_checks
load_song_dimension_table     >>    songs_quality_checks
load_time_dimension_table     >>    time_table_quality_checks
songs_quality_checks          >>    end_operator
users_quality_checks          >>    end_operator
artists_quality_checks        >>    end_operator
time_table_quality_checks     >>    end_operator
