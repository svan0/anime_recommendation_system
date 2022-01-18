"""
    Validation queries before moving from staging to processed
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

user_anime_pairs_unique_query = f"""
    SELECT
    IF(
        (SELECT COUNT(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}`)
        =
        (SELECT COUNT(*) FROM (
            SELECT user_id, anime_id 
            FROM`{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}`
            GROUP BY user_id, anime_id 
            )
        )
        , 'User anime pairs are unique',
        ERROR('User anime pairs are not unique')
    )
"""

user_anime_all_anime_known_query = f"""
    SELECT
    IF(
        (SELECT COUNT(*) FROM (
            (SELECT DISTINCT(anime_id) FROM `{PROJECT_ID}.{STAGING_DATASET}.{USER_ANIME_STAGING_TABLE}`)
            EXCEPT DISTINCT
            (SELECT anime_id FROM `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_STAGING_TABLE}`)
            )
        ) = 0,
        'All anime in user anime are known',
        ERROR('Not all anime in user anime are known')
    )
"""

anime_anime_pairs_unique_query = f"""
    SELECT
        IF(
            (SELECT COUNT(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_ANIME_STAGING_TABLE}`)
            =
            (SELECT COUNT(*) FROM (
                SELECT animeA, animeB 
                FROM`{PROJECT_ID}.{STAGING_DATASET}.{ANIME_ANIME_STAGING_TABLE}`
                GROUP BY animeA, animeB
                )
            )
            , 'Anime anime pairs are unique',
            ERROR('Anime anime pairs are not unique')
        )
"""

anime_anime_all_anime_known_query = f"""
    SELECT
    IF(
        (SELECT COUNT(*) FROM (
            (SELECT DISTINCT(animeA) FROM `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_ANIME_STAGING_TABLE}`)
            EXCEPT DISTINCT
            (SELECT anime_id FROM `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_STAGING_TABLE}`)
            )
        ) = 0,
        IF(
        (SELECT COUNT(*) FROM (
            (SELECT DISTINCT(animeB) FROM `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_ANIME_STAGING_TABLE}`)
            EXCEPT DISTINCT
            (SELECT anime_id FROM `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_STAGING_TABLE}`)
            )
        ) = 0,
        'All anime in anime anime are known',
        ERROR('Not all anime in anime anime are known')
        ),
        ERROR('Not all anime in user anime are known')
    )
"""