from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries

# User to provide connection ids after setting them via Airflow web app: 
aws_credentials_id='aws_credentials'
redshift_conn_id='redshift'

default_args = {
    'owner': 'student',
    'start_date': pendulum.now(),
    'depends_on_past': False, 
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        aws_credentials_id=aws_credentials_id, redshift_conn_id=redshift_conn_id, task_id='Stage_events', 
        create_table_query=SqlQueries.stage_events, prefix=Variable.get('s3_log_data_prefix'), 
        json_format="FORMAT JSON 's3://udacity-dend/log_json_path.json'", table='staging_events')

    stage_songs_to_redshift = StageToRedshiftOperator(aws_credentials_id=aws_credentials_id, 
        redshift_conn_id=redshift_conn_id, task_id='Stage_songs', 
        create_table_query=SqlQueries.stage_songs, prefix=Variable.get('s3_song_data_prefix'), 
        json_format="JSON 'auto'", table='staging_songs')

    load_songplays_table = LoadFactOperator(redshift_conn_id=redshift_conn_id,task_id='Load_songplays_fact_table', 
        table_query=SqlQueries.songplay_table_insert, table='songplays')

    # Dim tables insert mode param options: 'append' or 'truncate' ('truncate' is default)
    load_user_dimension_table = LoadDimensionOperator(
       redshift_conn_id=redshift_conn_id, task_id='Load_user_dim_table', table='users', 
       table_queries={"create": SqlQueries.user_table_create, "insert": SqlQueries.user_table_insert}, 
       insert_mode='truncate')

    load_song_dimension_table = LoadDimensionOperator(
        redshift_conn_id=redshift_conn_id,task_id='Load_song_dim_table', table='songs', 
        table_queries={"create": SqlQueries.song_table_create, "insert": SqlQueries.song_table_insert}, 
        insert_mode='truncate')

    load_artist_dimension_table = LoadDimensionOperator(
        redshift_conn_id=redshift_conn_id,task_id='Load_artist_dim_table', table='artists', 
        table_queries={"create": SqlQueries.artist_table_create, "insert": SqlQueries.artist_table_insert}, 
        insert_mode='truncate')

    load_time_dimension_table = LoadDimensionOperator(
        redshift_conn_id=redshift_conn_id,task_id='Load_time_dim_table', table='times',
        table_queries={"create": SqlQueries.time_table_create, "insert": SqlQueries.time_table_insert}, 
        insert_mode='truncate')

    # implemented 2 quality checks - demoed on songs table
    run_quality_checks = DataQualityOperator(
        redshift_conn_id=redshift_conn_id,task_id='Run_data_quality_checks', table='songs',
        table_queries={"row_count": SqlQueries.count_rows, "null_count": SqlQueries.count_nulls},
        column='song_id')

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift
    stage_songs_to_redshift >> load_songplays_table
    stage_events_to_redshift >> load_songplays_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table
    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator  

final_project_dag = final_project()

