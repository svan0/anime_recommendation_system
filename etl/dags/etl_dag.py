"""
    From landing area to processed area data pipeline
"""
import configparser
from pathlib import Path
import sys
from datetime import timedelta

sys.path.append(f"{Path(__file__).parents[0]}")

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago


from queries.staging_from_landing_queries import staging_anime_query
from queries.staging_from_landing_queries import staging_user_query
from queries.staging_from_landing_queries import staging_anime_anime_related_query,\
    staging_anime_anime_recommendation_query,\
    staging_anime_anime_all_query
from queries.staging_from_landing_queries import staging_user_anime_review_query,\
    staging_user_anime_activity_query,\
    staging_user_anime_watch_status_query,\
    staging_user_anime_favorite_query,\
    staging_user_anime_all_query

from queries.staging_plus_external_queries import user_anime_merge_external_data_query

from queries.staging_filtering_queries import anime_anime_remove_unknown_anime_query, user_anime_remove_unknown_anime_query
from queries.staging_filtering_queries import user_anime_remove_airing_completed_query,\
    user_anime_remove_not_yet_aired_not_plan_to_watch_query,\
    user_anime_remove_plan_to_watch_progress_not_0_query,\
    user_anime_remove_plan_to_watch_scored_query,\
    user_anime_remove_plan_to_watch_favorite_query,\
    user_anime_remove_progress_0_not_plan_to_watch_query,\
    user_anime_progress_0_status_null_to_plan_to_watch_query,\
    user_anime_remove_progress_all_not_completed_query,\
    user_anime_progress_all_status_null_to_completed_query,\
    user_anime_remove_progress_greater_num_episodes_query

from queries.staging_validation_queries import anime_anime_all_anime_known_query, anime_anime_pairs_unique_query
from queries.staging_validation_queries import user_anime_all_anime_known_query, user_anime_pairs_unique_query


config = configparser.ConfigParser()

config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))

PROJECT_ID = config.get('PROJECT', 'PROJECT_ID')
STAGING_DATASET = config.get('BQ_DATASETS', 'STAGING_DATASET') #staging_area
PROCESSED_DATASET = config.get('BQ_DATASETS', 'PROCESSED_DATASET') #processed_area

ANIME_STAGING_TABLE = config.get('STAGING_TABLE', 'ANIME')#anime
USER_STAGING_TABLE = config.get('STAGING_TABLE', 'USER')#user

REVIEW_STAGING_TABLE = config.get('STAGING_TABLE', 'REVIEW')#user_anime_review
ACTIVITY_STAGING_TABLE = config.get('STAGING_TABLE', 'ACTIVITY')#user_anime_activity
WATCH_STATUS_STAGING_TABLE = config.get('STAGING_TABLE', 'WATCH_STATUS')#user_anime_watch_status
FAVORITE_STAGING_TABLE = config.get('STAGING_TABLE', 'FAVORITE')#user_anime_favorite
USER_ANIME_STAGING_TABLE = config.get('STAGING_TABLE', 'USER_ANIME')#user_anime

RELATED_STAGING_TABLE = config.get('STAGING_TABLE', 'RELATED')#anime_anime_related
RECOMMENDED_STAGING_TABLE = config.get('STAGING_TABLE', 'RECOMMENDED')#anime_anime_recommendation
ANIME_ANIME_STAGING_TABLE = config.get('STAGING_TABLE', 'ANIME_ANIME')#anime_anime

ANIME_PROCESSED_TABLE = config.get('PROCESSED_TABLE', 'ANIME')#anime
USER_PROCESSED_TABLE = config.get('PROCESSED_TABLE', 'USER')#user
USER_ANIME_PROCESSED_TABLE = config.get('PROCESSED_TABLE', 'USER_ANIME')#user_anime
ANIME_ANIME_PROCESSED_TABLE = config.get('PROCESSED_TABLE', 'ANIME_ANIME')#anime_anime


default_args = {
    'owner': 'default_user',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries' : 1
}

dag = DAG(
    'anime_etl_pipeline',
    default_args=default_args
)

start_operator = DummyOperator(task_id='begin_execution', dag=dag)

#------------ Landing to staging ----------
anime_landing_to_staging = BigQueryOperator(
    sql=staging_anime_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{ANIME_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='anime_landing_to_staging',
    dag=dag
)

user_landing_to_staging = BigQueryOperator(
    sql=staging_user_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{USER_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='user_landing_to_staging',
    dag=dag
)

anime_anime_related_landing_to_staging = BigQueryOperator(
    sql=staging_anime_anime_related_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{RELATED_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='anime_anime_related_landing_to_staging',
    dag=dag
)
anime_anime_recommendation_landing_to_staging = BigQueryOperator(
    sql=staging_anime_anime_recommendation_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{RECOMMENDED_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='anime_anime_recommendation_landing_to_staging',
    dag=dag
)
anime_anime_intial_staging = BigQueryOperator(
    sql=staging_anime_anime_all_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{ANIME_ANIME_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='anime_anime_intial_staging',
    dag=dag
)

user_anime_review_landing_to_staging = BigQueryOperator(
    sql=staging_user_anime_review_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{REVIEW_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='user_anime_review_landing_to_staging',
    dag=dag
)
user_anime_watch_status_landing_to_staging = BigQueryOperator(
    sql=staging_user_anime_watch_status_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{WATCH_STATUS_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    task_id='user_anime_watch_status_landing_to_staging',
    use_legacy_sql=False,
    dag=dag
)
user_anime_activity_landing_to_staging = BigQueryOperator(
    sql=staging_user_anime_activity_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{ACTIVITY_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='user_anime_activity_landing_to_staging',
    dag=dag
)
user_anime_favorite_landing_to_staging = BigQueryOperator(
    sql=staging_user_anime_favorite_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{FAVORITE_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='user_anime_favorite_landing_to_staging',
    dag=dag
)
user_anime_initial_staging = BigQueryOperator(
    sql=staging_user_anime_all_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='user_anime_initial_staging',
    dag=dag
)

# ----------------- Merge external data -------
user_anime_merge_external_data = BigQueryOperator(
    sql=user_anime_merge_external_data_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='user_anime_merge_external_data',
    dag=dag
)


# ----------------- Data Cleaning -------------
anime_anime_remove_unknown_anime = BigQueryOperator(
    sql=anime_anime_remove_unknown_anime_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{ANIME_ANIME_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='anime_anime_remove_unknown_anime',
    dag=dag
)
user_anime_remove_unknown_anime = BigQueryOperator(
    sql=user_anime_remove_unknown_anime_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='user_anime_remove_unknown_anime',
    dag=dag
)

user_anime_remove_airing_completed = BigQueryOperator(
    sql=user_anime_remove_airing_completed_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='user_anime_remove_airing_completed',
    dag=dag
)
user_anime_remove_not_yet_aired_not_plan_to_watch = BigQueryOperator(
    sql=user_anime_remove_not_yet_aired_not_plan_to_watch_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='user_anime_remove_not_yet_aired_not_plan_to_watch',
    dag=dag
)
user_anime_remove_plan_to_watch_progress_not_0 = BigQueryOperator(
    sql=user_anime_remove_plan_to_watch_progress_not_0_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='user_anime_remove_plan_to_watch_progress_not_0',
    dag=dag
)
user_anime_remove_plan_to_watch_scored = BigQueryOperator(
    sql=user_anime_remove_plan_to_watch_scored_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='user_anime_remove_plan_to_watch_scored_query',
    dag=dag
)
user_anime_remove_plan_to_watch_favorite = BigQueryOperator(
    sql=user_anime_remove_plan_to_watch_favorite_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='user_anime_remove_plan_to_watch_favorite',
    dag=dag
)
user_anime_remove_progress_0_not_plan_to_watch = BigQueryOperator(
    sql=user_anime_remove_progress_0_not_plan_to_watch_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='user_anime_remove_progress_0_not_plan_to_watch',
    dag=dag
)
user_anime_progress_0_status_null_to_plan_to_watch = BigQueryOperator(
    sql=user_anime_progress_0_status_null_to_plan_to_watch_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='user_anime_progress_0_status_null_to_plan_to_watch',
    dag=dag
)
user_anime_progress_all_status_null_to_completed = BigQueryOperator(
    sql=user_anime_progress_all_status_null_to_completed_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='user_anime_progress_all_status_null_to_completed',
    dag=dag
)
user_anime_remove_progress_all_not_completed = BigQueryOperator(
    sql=user_anime_remove_progress_all_not_completed_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='user_anime_remove_progress_all_not_completed',
    dag=dag
)
user_anime_remove_progress_greater_num_episodes = BigQueryOperator(
    sql=user_anime_remove_progress_greater_num_episodes_query,
    destination_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='user_anime_remove_progress_greater_num_episodes',
    dag=dag
)

# ---------- Data Validation ----------
validation_anime_anime_all_anime_known = BigQueryOperator(
    sql=anime_anime_all_anime_known_query,
    use_legacy_sql=False,
    task_id='validation_anime_anime_all_anime_known',
    dag=dag
)
validation_anime_anime_pairs_unique = BigQueryOperator(
    sql=anime_anime_pairs_unique_query,
    use_legacy_sql=False,
    task_id='validation_anime_anime_pairs_unique',
    dag=dag
)
validation_user_anime_all_anime_known = BigQueryOperator(
    sql=user_anime_all_anime_known_query,
    use_legacy_sql=False,
    task_id='validation_user_anime_all_anime_known',
    dag=dag
)
validation_user_anime_pairs_unique = BigQueryOperator(
    sql=user_anime_pairs_unique_query,
    use_legacy_sql=False,
    task_id='validation_user_anime_pairs_unique',
    dag=dag
)

# ------------- Move to processed area ---------
anime_to_processed = BigQueryOperator(
    sql=f"SELECT * FROM `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_STAGING_TABLE}`",
    destination_dataset_table=f'{PROJECT_ID}.{PROCESSED_DATASET}.{ANIME_PROCESSED_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='anime_to_processed',
    dag=dag
)
user_to_processed = BigQueryOperator(
    sql=f"SELECT * FROM `{PROJECT_ID}.{STAGING_DATASET}.{USER_STAGING_TABLE}`",
    destination_dataset_table=f'{PROJECT_ID}.{PROCESSED_DATASET}.{USER_PROCESSED_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='user_to_processed',
    dag=dag
)
user_anime_to_processed = BigQueryOperator(
    sql=f"SELECT * FROM `{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}`",
    destination_dataset_table=f'{PROJECT_ID}.{PROCESSED_DATASET}.{USER_ANIME_PROCESSED_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='user_anime_to_processed',
    dag=dag
)
anime_anime_to_processed = BigQueryOperator(
    sql=f"SELECT * FROM `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_ANIME_STAGING_TABLE}`",
    destination_dataset_table=f'{PROJECT_ID}.{PROCESSED_DATASET}.{ANIME_ANIME_PROCESSED_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='anime_anime_to_processed',
    dag=dag
)

end_operator = DummyOperator(task_id='stop_execution', dag=dag)

start_operator >> [
    anime_landing_to_staging, 
    user_landing_to_staging,
    anime_anime_related_landing_to_staging,
    anime_anime_recommendation_landing_to_staging,
    user_anime_favorite_landing_to_staging,
    user_anime_activity_landing_to_staging,
    user_anime_watch_status_landing_to_staging,
    user_anime_review_landing_to_staging
]

[anime_anime_related_landing_to_staging, anime_anime_recommendation_landing_to_staging] >> anime_anime_intial_staging

[anime_landing_to_staging, anime_anime_intial_staging] >> anime_anime_remove_unknown_anime >> [validation_anime_anime_all_anime_known, validation_anime_anime_pairs_unique]

[
    user_anime_favorite_landing_to_staging, 
    user_anime_activity_landing_to_staging, 
    user_anime_watch_status_landing_to_staging,
    user_anime_review_landing_to_staging
] >> user_anime_initial_staging

user_anime_initial_staging >> user_anime_merge_external_data
user_anime_merge_external_data >> user_anime_remove_unknown_anime
user_anime_initial_staging >> user_anime_remove_unknown_anime
user_anime_remove_unknown_anime >> user_anime_progress_0_status_null_to_plan_to_watch
user_anime_progress_0_status_null_to_plan_to_watch >> user_anime_progress_all_status_null_to_completed
user_anime_progress_all_status_null_to_completed >> user_anime_remove_airing_completed
user_anime_remove_airing_completed >> user_anime_remove_not_yet_aired_not_plan_to_watch
user_anime_remove_not_yet_aired_not_plan_to_watch >> user_anime_remove_plan_to_watch_progress_not_0
user_anime_remove_plan_to_watch_progress_not_0 >> user_anime_remove_plan_to_watch_scored
user_anime_remove_plan_to_watch_scored >> user_anime_remove_plan_to_watch_favorite
user_anime_remove_plan_to_watch_favorite >> user_anime_remove_progress_0_not_plan_to_watch
user_anime_remove_progress_0_not_plan_to_watch >> user_anime_remove_progress_all_not_completed
user_anime_remove_progress_all_not_completed >> user_anime_remove_progress_greater_num_episodes
user_anime_remove_progress_greater_num_episodes >> validation_user_anime_all_anime_known
user_anime_remove_progress_greater_num_episodes >> validation_user_anime_pairs_unique


[
    validation_user_anime_all_anime_known,
    validation_user_anime_pairs_unique,
    validation_anime_anime_all_anime_known,
    validation_anime_anime_pairs_unique
] >> anime_to_processed
[
    validation_user_anime_all_anime_known,
    validation_user_anime_pairs_unique,
    validation_anime_anime_all_anime_known,
    validation_anime_anime_pairs_unique
] >> user_to_processed
[
    validation_user_anime_all_anime_known,
    validation_user_anime_pairs_unique,
    validation_anime_anime_all_anime_known,
    validation_anime_anime_pairs_unique
] >> anime_anime_to_processed
[
    validation_user_anime_all_anime_known,
    validation_user_anime_pairs_unique,
    validation_anime_anime_all_anime_known,
    validation_anime_anime_pairs_unique
] >> user_anime_to_processed

[
    anime_to_processed,
    user_to_processed,
    anime_anime_to_processed,
    user_anime_to_processed
] >> end_operator

