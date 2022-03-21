import configparser
from pathlib import Path

config = configparser.ConfigParser()

config.read_file(open(f"{Path(__file__).parents[0].parents[0]}/config.cfg"))

PROJECT_ID = config.get('PROJECT', 'PROJECT_ID')
STAGING_DATASET = config.get('BQ_DATASETS', 'STAGING_DATASET') #staging_area

ANIME_STAGING_TABLE = config.get('STAGING_TABLE', 'ANIME') #anime
USER_STAGING_TABLE = config.get('STAGING_TABLE', 'USER') #user

REVIEW_STAGING_TABLE = config.get('STAGING_TABLE', 'REVIEW') #user_anime_review
ACTIVITY_STAGING_TABLE = config.get('STAGING_TABLE', 'ACTIVITY') #user_anime_activity
WATCH_STATUS_STAGING_TABLE = config.get('STAGING_TABLE', 'WATCH_STATUS') #user_anime_watch_status
FAVORITE_STAGING_TABLE = config.get('STAGING_TABLE', 'FAVORITE') #user_anime_favorite
USER_ANIME_STAGING_TABLE = config.get('STAGING_TABLE', 'USER_ANIME') #user_anime

RELATED_STAGING_TABLE = config.get('STAGING_TABLE', 'RELATED') #anime_anime_related
RECOMMENDED_STAGING_TABLE = config.get('STAGING_TABLE', 'RECOMMENDED') #anime_anime_recommendation
ANIME_ANIME_STAGING_TABLE = config.get('STAGING_TABLE', 'ANIME_ANIME') #anime_anime

FRIENDS_STAGING_TABLE = config.get('STAGING_TABLE', 'FRIENDS') #user_user_friends
USER_USER_STAGING_TABLE = config.get('STAGING_TABLE', 'USER_USER') #user_user

staging_user_anime_all_query = f"""
    WITH 
    favorite AS (
        SELECT *, 1 AS favorite FROM `{PROJECT_ID}.{STAGING_DATASET}.{FAVORITE_STAGING_TABLE}`
    )
    SELECT  COALESCE(A.user_id, B.user_id, C.user_id, D.user_id) AS user_id,
            COALESCE(A.anime_id, B.anime_id, C.anime_id, D.anime_id) AS anime_id,
            COALESCE(B.favorite, 0) AS favorite,
            A.review_id,
            A.review_date,
            A.num_useful AS review_num_useful,
            A.overall_score AS review_score,
            A.story_score AS review_story_score, 
            A.animation_score AS review_animation_score, 
            A.sound_score AS review_sound_score, 
            A.character_score AS review_character_score, 
            A.enjoyment_score AS review_enjoyment_score,
            COALESCE(D.score, 
                     A.overall_score,
                     IF (COALESCE(B.favorite, 0) = 1, 10, NULL)
            ) AS score,
            COALESCE(D.status, 
                     C.activity_type, 
                     IF (E.status IS NOT NULL AND E.status = 'Currently Airing', 'watching', NULL),
                     IF (E.status IS NOT NULL AND E.status = 'Not yet aired', 'plan_to_watch', NULL),
                     IF (E.status IS NOT NULL AND E.num_episodes IS NOT NULL AND D.progress IS NOT NULL AND E.status = 'Finished Airing' AND E.num_episodes = D.progress, 'completed', NULL),
                     IF (A.review_id IS NOT NULL OR COALESCE(B.favorite, 0) = 1, 'completed', NULL)
            ) AS status,
            COALESCE(D.progress,
                     IF (D.status IS NOT NULL AND D.status = 'completed', E.num_episodes, NULL),
                     IF (C.activity_type IS NOT NULL AND C.activity_type = 'completed', E.num_episodes, NULL)
            ) AS progress,
            GREATEST(COALESCE(
                        GREATEST(C.activity_date, A.review_date), 
                        C.activity_date, 
                        A.review_date,
                        IF (E.status IS NOT NULL AND E.status = 'Currently Airing', F.last_online_date, NULL),
                        IF (E.status IS NOT NULL AND E.status = 'Finished Airing', LEAST(F.last_online_date, E.end_date), NULL)
            ), DATETIME('2004-05-11')) AS last_interaction_date
    FROM `{PROJECT_ID}.{STAGING_DATASET}.{REVIEW_STAGING_TABLE}` A
    FULL OUTER JOIN favorite B
    ON A.user_id = B.user_id AND A.anime_id = B.anime_id
    FULL OUTER JOIN `{PROJECT_ID}.{STAGING_DATASET}.{ACTIVITY_STAGING_TABLE}` C
    ON COALESCE(A.user_id, B.user_id) = C.user_id AND COALESCE(A.anime_id, B.anime_id) = C.anime_id
    FULL OUTER JOIN `{PROJECT_ID}.{STAGING_DATASET}.{WATCH_STATUS_STAGING_TABLE}` D
    ON COALESCE(A.user_id, B.user_id, C.user_id) = D.user_id AND COALESCE(A.anime_id, B.anime_id, C.anime_id) = D.anime_id
    LEFT JOIN `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_STAGING_TABLE}` E
    ON COALESCE(A.anime_id, B.anime_id, C.anime_id, D.anime_id) = E.anime_id
    LEFT JOIN `{PROJECT_ID}.{STAGING_DATASET}.{USER_STAGING_TABLE}` F
    ON COALESCE(A.user_id, B.user_id, C.user_id, D.user_id) = F.user_id
"""

staging_anime_anime_all_query = f"""
    WITH 
    recommendation AS (
        SELECT *, 1 AS recommendation FROM `{PROJECT_ID}.{STAGING_DATASET}.{RECOMMENDED_STAGING_TABLE}`
    ),
    related AS (
        SELECT animeA, animeB, relation_type, 1 AS related FROM `{PROJECT_ID}.{STAGING_DATASET}.{RELATED_STAGING_TABLE}` WHERE animeA <> animeB
        UNION DISTINCT
        SELECT anime_id AS animeA, anime_id AS animeB, 'Identity' AS relation_type, 1 AS related FROM `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_STAGING_TABLE}`
    ),
    sequel_count AS (
        SELECT animeA AS anime_id, COUNT(*) AS sequel_count
        FROM `{PROJECT_ID}.{STAGING_DATASET}.{RELATED_STAGING_TABLE}`
        WHERE relation_type = 'Sequel'
        GROUP BY anime_id
    ),
    related_sequel_count AS (
        SELECT A.animeA, A.animeB, B.sequel_count
        FROM 
            (   
                SELECT animeA, animeB FROM `{PROJECT_ID}.{STAGING_DATASET}.{RELATED_STAGING_TABLE}`
                UNION DISTINCT
                SELECT anime_id AS animeA, anime_id AS animeB FROM `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_STAGING_TABLE}`
            ) A
        LEFT JOIN sequel_count B
        ON A.animeB = B.anime_id
    ),
    related_ordered AS (
        SELECT A.animeA, A.animeB, ROW_NUMBER() OVER (PARTITION BY animeA ORDER BY A.sequel_count DESC, B.total_count DESC) AS related_order
        FROM related_sequel_count A
        LEFT JOIN `{PROJECT_ID}.{STAGING_DATASET}.{ANIME_STAGING_TABLE}` B
        ON A.animeB = B.anime_id
    )
    SELECT 
        COALESCE(A.animeA, B.animeA, C.animeA) AS animeA,
        COALESCE(A.animeB, B.animeB, C.animeB) AS animeB,
        COALESCE(A.recommendation, 0) AS recommendation,
        A.recommendation_url,
        A.num_recommenders,
        COALESCE(B.related, 0) AS related,
        B.relation_type,
        C.related_order
    FROM recommendation A
    FULL OUTER JOIN related B
    ON A.animeA = B.animeA AND A.animeB = B.animeB
    FULL OUTER JOIN related_ordered C
    ON C.animeA = COALESCE(A.animeA, B.animeA) AND C.animeB = COALESCE(A.animeB, B.animeB)
"""

staging_user_user_all_query = f"""
    SELECT userA, userB, 1 AS friends, friendship_date
    FROM `{PROJECT_ID}.{STAGING_DATASET}.{FRIENDS_STAGING_TABLE}`
"""