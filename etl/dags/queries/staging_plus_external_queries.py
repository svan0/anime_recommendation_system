"""
    Merge crawled data with external data
"""
import configparser
from pathlib import Path

config = configparser.ConfigParser()

config.read_file(open(f"{Path(__file__).parents[0].parents[0]}/config.cfg"))

PROJECT_ID = config.get('PROJECT', 'PROJECT_ID')
STAGING_DATASET = config.get('BQ_DATASETS', 'STAGING_DATASET') #staging_area
EXTERNAL_DATASET = config.get('BQ_DATASETS', 'EXTERNAL_DATASET')#external_data

USER_ANIME_STAGING_TABLE = config.get('STAGING_TABLE', 'USER_ANIME')#user_anime
USER_ANIME_EXTERNAL_TABLE = config.get('EXTERNAL_TABLE', 'USER_ANIME')#user_anime

user_anime_merge_external_data_query = f"""
    WITH 
    external_user_anime AS (
        SELECT  username AS user_id,
                anime_id,
                CASE my_score WHEN 0 THEN NULL ELSE my_score END AS score,
                CASE my_status
                    WHEN "1" THEN "watching"
                    WHEN "2" THEN "completed"
                    WHEN "3" THEN "on_hold"
                    WHEN "4" THEN "dropped"
                    WHEN "6" THEN "plan_to_watch"
                    ELSE NULL
                END AS status,
                my_watched_episodes AS progress,
                CAST(my_last_updated AS DATETIME) AS last_interaction_date
        FROM `{PROJECT_ID}.{EXTERNAL_DATASET}.{USER_ANIME_EXTERNAL_TABLE}`
    )
    SELECT  
        COALESCE(A.user_id, B.user_id) AS user_id,
        COALESCE(A.anime_id, B.anime_id) AS anime_id,
        COALESCE(A.favorite, 0) AS favorite,
        A.review_id,
        A.review_date,
        A.review_num_useful,
        A.review_score,
        A.review_story_score,
        A.review_animation_score,
        A.review_sound_score,
        A.review_character_score,
        A.review_enjoyment_score,
        A.last_activity_type,
        COALESCE(A.score, B.score) AS score,
        COALESCE(A.status, B.status) AS status,
        COALESCE(A.progress, B.progress) AS progress,
        COALESCE(GREATEST(A.last_interaction_date, B.last_interaction_date), A.last_interaction_date, B.last_interaction_date) AS last_interaction_date
    FROM `{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}` A
    FULL OUTER JOIN external_user_anime B
    ON A.user_id = B.user_id AND A.anime_id = B.anime_id
"""

