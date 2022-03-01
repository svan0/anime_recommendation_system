"""
    Data cleaning queries
"""
import configparser
from pathlib import Path

config = configparser.ConfigParser()

config.read_file(open(f"{Path(__file__).parents[0].parents[0]}/config.cfg"))

PROJECT_ID = config.get('PROJECT', 'PROJECT_ID')
STAGING_DATASET = config.get('BQ_DATASETS', 'STAGING_DATASET') #staging_area

#-------- Staging Area Tables -----------
ANIME_STAGING_TABLE = config.get('STAGING_TABLE', 'ANIME')#anime
USER_STAGING_TABLE = config.get('STAGING_TABLE', 'USER')#user
USER_ANIME_STAGING_TABLE = config.get('STAGING_TABLE', 'USER_ANIME')#user_anime
ANIME_ANIME_STAGING_TABLE = config.get('STAGING_TABLE', 'ANIME_ANIME')#anime_anime
USER_USER_STAGING_TABLE = config.get('STAGING_TABLE', 'USER_USER')#user_user


# ------ Unknown Anime ---------
anime_anime_remove_unknown_anime_query = f"""
    SELECT A.*
    FROM `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_ANIME_STAGING_TABLE}` A
    INNER JOIN `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_STAGING_TABLE}` B
    ON A.animeA = B.anime_id
    INNER JOIN `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_STAGING_TABLE}` C
    ON A.animeB = C.anime_id
"""

user_anime_remove_unknown_anime_query = f"""
    SELECT A.*
    FROM `{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}` A
    INNER JOIN `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_STAGING_TABLE}` B
    ON A.anime_id = B.anime_id
"""

# ------ Unknown User ---------
user_user_remove_unknown_user_query = f"""
    SELECT A.*
    FROM `{PROJECT_ID}.{STAGING_DATASET}.{USER_USER_STAGING_TABLE}` A
    INNER JOIN `{PROJECT_ID}.{STAGING_DATASET}.{USER_STAGING_TABLE}` B
    ON A.userA = B.user_id
    INNER JOIN `{PROJECT_ID}.{STAGING_DATASET}.{USER_STAGING_TABLE}` C
    ON A.userB = C.user_id
"""

user_anime_remove_unknown_user_query = f"""
    SELECT A.*
    FROM `{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}` A
    INNER JOIN `{PROJECT_ID}.{STAGING_DATASET}.{USER_STAGING_TABLE}` B
    ON A.user_id = B.user_id
"""

# ------ Progress ---------
user_anime_progress_0_status_null_to_plan_to_watch_query = f"""
    SELECT 
        user_id,
        anime_id,
        favorite,
        review_id,
        review_date,
        review_num_useful,
        review_score,
        review_story_score,
        review_animation_score,
        review_sound_score,
        review_character_score,
        review_enjoyment_score,
        score,
        COALESCE(status, CASE progress WHEN 0 THEN "plan_to_watch" ELSE NULL END) AS status,
        COALESCE(progress, CASE status WHEN "plan_to_watch" THEN 0 ELSE NULL END) AS progress,
        last_interaction_date
    FROM `{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}`
"""

user_anime_remove_progress_0_not_plan_to_watch_query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}`
    WHERE NOT(progress IS NOT NULL AND progress = 0 AND status IS NOT NULL AND status <> 'plan_to_watch')
"""

user_anime_progress_all_status_null_to_completed_query = f"""
    SELECT 
        A.user_id,
        A.anime_id,
        A.favorite,
        A.review_id,
        A.review_date,
        A.review_num_useful,
        A.review_score,
        A.review_story_score,
        A.review_animation_score,
        A.review_sound_score,
        A.review_character_score,
        A.review_enjoyment_score,
        A.score,
        COALESCE(
            A.status, 
            IF(A.progress IS NOT NULL AND B.num_episodes IS NOT NULL AND A.progress = B.num_episodes, 'completed', NULL)
        ) AS status,
        COALESCE(
            A.progress,
            IF(A.status IS NOT NULL AND B.num_episodes IS NOT NULL AND A.status = 'completed', B.num_episodes, NULL)
        ) AS progress,
        A.last_interaction_date
    FROM `{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}` A
    LEFT JOIN `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_STAGING_TABLE}` B
    ON A.anime_id = B.anime_id
"""



user_anime_remove_progress_all_not_completed_query = f"""
    SELECT A.*
    FROM `{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}` A
    LEFT JOIN `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_STAGING_TABLE}` B
    ON A.anime_id = B.anime_id
    WHERE NOT(A.progress IS NOT NULL AND B.num_episodes IS NOT NULL AND A.progress = B.num_episodes AND A.status IS NOT NULL AND A.status <> 'completed')
"""

user_anime_remove_progress_greater_num_episodes_query = f"""
    SELECT A.*
    FROM `{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}` A
    LEFT JOIN `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_STAGING_TABLE}` B
    ON A.anime_id = B.anime_id
    WHERE NOT(A.progress IS NOT NULL AND B.num_episodes IS NOT NULL AND A.progress > B.num_episodes)
"""

# -------- Anime Status ----------------

user_anime_remove_not_yet_aired_not_plan_to_watch_query = f"""
    SELECT A.*
    FROM `{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}` A
    LEFT JOIN `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_STAGING_TABLE}` B
    ON A.anime_id = B.anime_id
    WHERE NOT(B.status = 'Not yet aired' AND A.status IS NOT NULL AND A.status <> 'plan_to_watch')
"""

user_anime_remove_airing_completed_query = f"""
    SELECT A.*
    FROM `{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}` A
    LEFT JOIN `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_STAGING_TABLE}` B
    ON A.anime_id = B.anime_id
    WHERE NOT(B.status = 'Airing' AND A.status = 'completed')
"""

# ------ User Anime Status ---------

user_anime_remove_plan_to_watch_progress_not_0_query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}`
    WHERE NOT(status IS NOT NULL AND status = 'plan_to_watch' AND progress IS NOT NULL AND progress <> 0)
"""

user_anime_remove_plan_to_watch_scored_query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}`
    WHERE NOT(status IS NOT NULL AND status = 'plan_to_watch' AND score IS NOT NULL)
"""

user_anime_remove_plan_to_watch_favorite_query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}`
    WHERE NOT(status IS NOT NULL AND status = 'plan_to_watch' AND favorite = 1)
"""