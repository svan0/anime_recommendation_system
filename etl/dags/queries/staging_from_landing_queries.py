"""
    Landing area to stagng area queries
"""
import configparser
from pathlib import Path

config = configparser.ConfigParser()

config.read_file(open(f"{Path(__file__).parents[0].parents[0]}/config.cfg"))

PROJECT_ID = config.get('PROJECT', 'PROJECT_ID')
LANDING_DATASET = config.get('BQ_DATASETS', 'LANDING_DATASET') #landing_area
STAGING_DATASET = config.get('BQ_DATASETS', 'STAGING_DATASET') #staging_area

#-------- Landing Area Tables -----------
ANIME_ITEM_LANDING_TABLE = config.get('LANDING_TABLE', 'ANIME_ITEM') #anime_item
PROFILE_ITEM_LANDING_TABLE = config.get('LANDING_TABLE', 'PROFILE_ITEM') #profile_item

ACTIVITY_ITEM_LANDING_TABLE = config.get('LANDING_TABLE', 'ACTIVITY_ITEM') #activity_item
WATCH_STATUS_ITEM_LANDING_TABLE = config.get('LANDING_TABLE', 'WATCH_STATUS_ITEM') #watch_status_item
REVIEW_ITEM_LANDING_TABLE = config.get('LANDING_TABLE', 'REVIEW_ITEM') #review_item
FAVORITE_ITEM_LANDING_TABLE = config.get('LANDING_TABLE', 'FAVORITE_ITEM') #favorite_item

RELATED_ANIME_ITEM_LANDING_TABLE = config.get('LANDING_TABLE', 'RELATED_ANIME_ITEM') #related_anime_item
RECOMMENDED_ANIME_ITEM_LANDING_TABLE = config.get('LANDING_TABLE', 'RECOMMENDED_ANIME_ITEM') #recommendation_item

FRIENDS_ITEM_LANDING_TABLE = config.get('LANDING_TABLE', 'FRIENDS_ITEM') #friends_item

#-------- Staging Area Tables -----------
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

#------------- Staging Anime Queries ---------------
staging_anime_query = f"""
    WITH ranked_anime_by_crawl_date AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY uid ORDER BY crawl_date DESC) AS row_number
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{ANIME_ITEM_LANDING_TABLE}`
    )
    SELECT  
        uid AS anime_id,
        url	AS anime_url,
        title,
        synopsis,
        main_pic,
        type,
        source_type,
        num_episodes,
        status,
        start_date,
        end_date,
        season,
        studios,
        genres,
        score,
        score_count,
        score_rank,
        popularity_rank,
        members_count,
        favorites_count,
        watching_count,
        completed_count,
        on_hold_count,
        dropped_count,
        plan_to_watch_count,
        total_count,
        score_10_count,
        score_09_count,
        score_08_count,
        score_07_count,
        score_06_count,
        score_05_count,
        score_04_count,
        score_03_count,
        score_02_count,
        score_01_count,
        clubs,
        pics
    FROM ranked_anime_by_crawl_date
    WHERE row_number = 1
"""

#------------- Staging User Queries ----------------
staging_user_query = f"""
    WITH ranked_profile_by_crawl_date AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY LOWER(uid) ORDER BY crawl_date DESC) AS row_number
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{PROFILE_ITEM_LANDING_TABLE}`
    )
    SELECT 
        LOWER(uid) AS user_id, 
        url AS user_url, 
        last_online_date, 
        num_watching,
        num_completed,
        num_on_hold,
        num_dropped,
        num_plan_to_watch,
        num_days,
        mean_score,
        clubs
    FROM ranked_profile_by_crawl_date
    WHERE row_number = 1
"""

#------------- Staging User Anime Queries ----------
staging_user_anime_activity_query = f"""
    WITH latest_activity_user_item AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY LOWER(user_id), anime_id ORDER BY date DESC, crawl_date DESC) AS row_number
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{ACTIVITY_ITEM_LANDING_TABLE}`
    )
    SELECT 
        LOWER(user_id) AS user_id, 
        anime_id, 
        activity_type, 
        date AS activity_date
    FROM latest_activity_user_item
    WHERE row_number = 1
"""

staging_user_anime_watch_status_query = f"""
    WITH ranked_watch_status_by_crawl_date AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY LOWER(user_id), anime_id ORDER BY crawl_date DESC) AS row_number
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{WATCH_STATUS_ITEM_LANDING_TABLE}`
    )
    SELECT 
        LOWER(user_id) AS user_id, 
        anime_id, 
        status, 
        score, 
        progress
    FROM ranked_watch_status_by_crawl_date
    WHERE row_number = 1
"""

staging_user_anime_review_query = f"""
    WITH ranked_review_by_crawl_date AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY LOWER(user_id), anime_id ORDER BY crawl_date DESC) AS row_number
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{REVIEW_ITEM_LANDING_TABLE}`
    )
    SELECT 
        LOWER(user_id) AS user_id, 
        anime_id, 
        uid AS review_id, 
        review_date, 
        num_useful, 
        overall_score, 
        story_score, 
        animation_score, 
        sound_score, 
        character_score, 
        enjoyment_score
    FROM ranked_review_by_crawl_date
    WHERE row_number = 1
"""

staging_user_anime_favorite_query = f"""
    WITH user_last_crawl_date AS (
        SELECT LOWER(user_id) AS user_id, MAX(crawl_date) AS last_crawl_date
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{FAVORITE_ITEM_LANDING_TABLE}`
        GROUP BY LOWER(user_id)
    )
    SELECT DISTINCT LOWER(A.user_id) AS user_id, A.anime_id
    FROM `{PROJECT_ID}.{LANDING_DATASET}.{FAVORITE_ITEM_LANDING_TABLE}` A
    JOIN user_last_crawl_date B
    ON LOWER(A.user_id) = B.user_id AND A.crawl_date = B.last_crawl_date
"""

#------------- Staging Anime Anime Queries ----------
staging_anime_anime_related_query = f"""
    WITH RECURSIVE src_anime_last_crawl_date AS (
        SELECT src_anime, MAX(crawl_date) AS last_crawl_date
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{RELATED_ANIME_ITEM_LANDING_TABLE}`
        GROUP BY src_anime
    ),
    last_crawl_related_anime AS (
        SELECT A.src_anime, A.dest_anime, A.relation_type
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{RELATED_ANIME_ITEM_LANDING_TABLE}` A
        JOIN src_anime_last_crawl_date B
        ON A.src_anime = B.src_anime AND A.crawl_date = B.last_crawl_date 
    ),
    related_level_1 AS (
        SELECT src_anime AS animeA, dest_anime AS animeB, relation_type
        FROM last_crawl_related_anime
        GROUP BY src_anime, dest_anime, relation_type
    ),
    related AS (
        SELECT animeA, animeB, relation_type, 1 AS level FROM related_level_1
        UNION ALL
        (
            SELECT A.animeA, 
                B.animeB, 
                COALESCE(IF(A.relation_type='Sequel' AND B.relation_type IS NOT NULL AND B.relation_type='Sequel', 'Sequel', NULL),
                        IF(A.relation_type='Prequel' AND B.relation_type IS NOT NULL AND B.relation_type='Prequel', 'Prequel', NULL),
                        IF(B.level < 5 AND (A.relation_type='Parent story' OR (B.relation_type IS NOT NULL AND B.relation_type='Parent story')), 'Parent story', NULL),
                        IF(B.level < 5 AND (A.relation_type='Side story' OR (B.relation_type IS NOT NULL AND B.relation_type='Side story')), 'Side story', NULL),
                        IF(B.level < 5 AND (A.relation_type='Other' OR (B.relation_type IS NOT NULL AND B.relation_type='Other')), 'Other', NULL),
                        IF(B.level < 5 AND (A.relation_type='Spin-off' OR (B.relation_type IS NOT NULL AND B.relation_type='Spin-off')), 'Spin-off', NULL),
                        IF(B.level < 5 AND (A.relation_type='Alternative setting' OR (B.relation_type IS NOT NULL AND B.relation_type='Alternative setting')), 'Alternative setting', NULL),
                        IF(B.level < 5 AND (A.relation_type='Alternative version' OR (B.relation_type IS NOT NULL AND B.relation_type='Alternative version')), 'Alternative version', NULL),
                        IF(B.level < 5 AND (A.relation_type='Full story' OR (B.relation_type IS NOT NULL AND B.relation_type='Full story')), 'Full story', NULL),
                        IF(B.level < 5 AND (A.relation_type='Summary' OR (B.relation_type IS NOT NULL AND B.relation_type='Summary')), 'Summary', NULL),
                        IF(B.level < 5 AND (A.relation_type='Character' OR (B.relation_type IS NOT NULL AND B.relation_type='Character')), 'Character', NULL)
                ) AS relation_type,
                B.level + 1 AS level
            FROM related_level_1 A
            INNER JOIN related B
            ON A.animeB = B.animeA
            WHERE B.level + 1 <= 50 AND (B.level < 5 OR (A.relation_type='Sequel' AND B.relation_type IS NOT NULL AND B.relation_type='Sequel') OR (A.relation_type='Prequel' AND B.relation_type IS NOT NULL AND B.relation_type='Prequel'))
        )
    ),
    order_rem_duplicates AS (
        SELECT animeA, animeB, relation_type, ROW_NUMBER() OVER (PARTITION BY animeA, animeB ORDER BY level ASC) AS row_number 
        FROM related 
        WHERE relation_type IS NOT NULL
    )
    SELECT animeA, animeB, relation_type FROM order_rem_duplicates WHERE row_number = 1
"""

staging_anime_anime_recommendation_query = f"""
    WITH src_anime_last_crawl_date AS (
        SELECT src_anime, MAX(crawl_date) AS last_crawl_date
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{RECOMMENDED_ANIME_ITEM_LANDING_TABLE}`
        GROUP BY src_anime
    ),
    last_crawl_recommendation AS (
        SELECT A.src_anime, A.dest_anime, A.url, A.num_recs, A.crawl_date
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{RECOMMENDED_ANIME_ITEM_LANDING_TABLE}` A
        LEFT JOIN src_anime_last_crawl_date B
        ON A.src_anime = B.src_anime 
        WHERE A.crawl_date = B.last_crawl_date 
    ),
    fixed_both_directions AS (
        SELECT animeA, animeB, url AS recommendation_url, num_recs AS num_recommenders, crawl_date, ROW_NUMBER() OVER (PARTITION BY animeA, animeB ORDER BY crawl_date DESC) AS row_number
        FROM (
            (SELECT src_anime AS animeA, dest_anime AS animeB, url, num_recs, crawl_date FROM last_crawl_recommendation)
            UNION DISTINCT
            (SELECT dest_anime AS animeA, src_anime AS animeB, url, num_recs, crawl_date FROM last_crawl_recommendation)
        )
    )
    SELECT animeA, animeB, recommendation_url, num_recommenders FROM fixed_both_directions WHERE row_number = 1
"""

#------------- Staging User User Queries ----------
staging_user_user_friends_query = f"""
    WITH src_profile_last_crawl_date AS (
        SELECT LOWER(src_profile) AS src_profile, MAX(crawl_date) AS last_crawl_date
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{FRIENDS_ITEM_LANDING_TABLE}`
        GROUP BY LOWER(src_profile)
    ),
    last_crawl_friendship AS (
        SELECT LOWER(A.src_profile) AS userA, LOWER(A.dest_profile) AS userB, A.friendship_date, A.crawl_date
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{FRIENDS_ITEM_LANDING_TABLE}` A
        JOIN src_profile_last_crawl_date B
        ON LOWER(A.src_profile) = B.src_profile AND A.crawl_date = B.last_crawl_date
    ),
    fixed_both_directions AS (
        SELECT userA, userB, friendship_date, crawl_date, ROW_NUMBER() OVER (PARTITION BY userA, userB ORDER BY crawl_date DESC) AS row_number
        FROM (
            (SELECT userA, userB, friendship_date, crawl_date FROM last_crawl_friendship)
            UNION DISTINCT
            (SELECT userA, userB, friendship_date, crawl_date FROM last_crawl_friendship)
        )
    )
    SELECT userA, userB, friendship_date FROM fixed_both_directions WHERE row_number = 1
"""
