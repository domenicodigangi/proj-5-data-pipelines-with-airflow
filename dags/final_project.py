from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries

default_args = {
    "owner": "udacity",
    "depends_on_past": False,
    "start_date": pendulum.now(),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}


@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
)
def final_project():

    start_operator = DummyOperator(task_id="Begin_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        redshift_conn_id="redshift",
        s3_path="s3://udacity-ddg-proj-airflow/log_data",
        table_name="staging_events",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        redshift_conn_id="redshift",
        s3_path="s3://udacity-ddg-proj-airflow/song_data",
        table_name="staging_songs",
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table", conn_id="redshift"
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table", dimension="user", conn_id="redshift"
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table", dimension="song", conn_id="redshift"
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table", dimension="artist", conn_id="redshift"
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table", dimension="time", conn_id="redshift"
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        conn_id="redshift",
        sql_test_cases="select count(*) from artist where artist_id is null",
        expected_result=0,
    )

    stage_events_to_redshift >> stage_songs_to_redshift
    stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table
    load_user_dimension_table >> run_quality_checks


final_project_dag = final_project()
