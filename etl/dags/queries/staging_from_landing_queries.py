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
PROFILE_ITEM_LANDING_TABLE = config.get('LANDING_TABLE', 'PROFILE_ITEM')#profile_item

ACTIVITY_ITEM_LANDING_TABLE = config.get('LANDING_TABLE', 'ACTIVITY_ITEM')#activity_item
WATCH_STATUS_ITEM_LANDING_TABLE = config.get('LANDING_TABLE', 'WATCH_STATUS_ITEM')#watch_status_item
REVIEW_ITEM_LANDING_TABLE = config.get('LANDING_TABLE', 'REVIEW_ITEM')#review_item
FAVORITE_ITEM_LANDING_TABLE = config.get('LANDING_TABLE', 'FAVORITE_ITEM')#favorite_item

RELATED_ANIME_ITEM_LANDING_TABLE = config.get('LANDING_TABLE', 'RELATED_ANIME_ITEM')#related_anime_item
RECOMMENDED_ANIME_ITEM_LANDING_TABLE = config.get('LANDING_TABLE', 'RECOMMENDED_ANIME_ITEM')#recommendation_item

#-------- Staging Area Tables -----------
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


#------------- Staging Anime Queries ---------------
staging_anime_query = f"""
    WITH ranked_anime_by_crawl_date AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY uid ORDER BY crawl_date DESC) AS row_number
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{ANIME_ITEM_LANDING_TABLE}`
    )
    SELECT  
        uid AS anime_id,
        url	AS anime_url,
        title
        synopsis,
        main_pic
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
        SELECT *, ROW_NUMBER() OVER (PARTITION BY uid ORDER BY crawl_date DESC) AS row_number
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{PROFILE_ITEM_LANDING_TABLE}`
    )
    SELECT 
        uid AS user_id, 
        url AS user_url, 
        last_online_date, 
        num_forum_posts, 
        num_reviews, 
        num_recommendations, 
        num_blog_posts, 
        num_days AS num_days_watching_anime, 
        mean_score, 
        clubs
    FROM ranked_profile_by_crawl_date
    WHERE row_number = 1
"""

#------------- Staging User Anime Queries ----------
staging_user_anime_activity_query = f"""
    WITH latest_activity_user_item AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id, anime_id ORDER BY date DESC, crawl_date DESC) AS row_number
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{ACTIVITY_ITEM_LANDING_TABLE}`
    )
    SELECT 
        user_id, 
        anime_id, 
        activity_type, 
        date AS activity_date
    FROM latest_activity_user_item
    WHERE row_number = 1
"""

staging_user_anime_watch_status_query = f"""
    WITH ranked_watch_status_by_crawl_date AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id, anime_id ORDER BY crawl_date DESC) AS row_number
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{WATCH_STATUS_ITEM_LANDING_TABLE}`
    )
    SELECT 
        user_id, 
        anime_id, 
        status, 
        score, 
        progress
    FROM ranked_watch_status_by_crawl_date
    WHERE row_number = 1
"""

staging_user_anime_review_query = f"""
    WITH ranked_review_by_crawl_date AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id, anime_id ORDER BY crawl_date DESC) AS row_number
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{REVIEW_ITEM_LANDING_TABLE}`
    )
    SELECT 
        user_id, 
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
        SELECT user_id, MAX(crawl_date) AS last_crawl_date
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{FAVORITE_ITEM_LANDING_TABLE}`
        GROUP BY user_id
    )
    SELECT A.user_id, A.anime_id
    FROM `{PROJECT_ID}.{LANDING_DATASET}.{FAVORITE_ITEM_LANDING_TABLE}` A
    JOIN user_last_crawl_date B
    ON A.user_id = B.user_id AND A.crawl_date = B.last_crawl_date
"""

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
            C.activity_type AS last_activity_type,
            C.activity_date AS last_activity_date,
            COALESCE(D.score, 
                     A.overall_score,
                     IF (COALESCE(B.favorite, 0) = 1, 10, NULL)
            ) AS score,
            COALESCE(D.status, 
                     C.activity_type, 
                     IF (A.review_id IS NOT NULL OR COALESCE(B.favorite, 0) = 1, 'completed', NULL)
            ) AS status,
            D.progress,
            COALESCE(GREATEST(C.activity_date, A.review_date), C.activity_date, A.review_date) AS last_interaction_date
    FROM `{PROJECT_ID}.{STAGING_DATASET}.{REVIEW_STAGING_TABLE}` A
    FULL OUTER JOIN favorite B
    ON A.user_id = B.user_id AND A.anime_id = B.anime_id
    FULL OUTER JOIN `{PROJECT_ID}.{STAGING_DATASET}.{ACTIVITY_STAGING_TABLE}` C
    ON COALESCE(A.user_id, B.user_id) = C.user_id AND COALESCE(A.anime_id, B.anime_id) = C.anime_id
    FULL OUTER JOIN `{PROJECT_ID}.{STAGING_DATASET}.{WATCH_STATUS_STAGING_TABLE}` D
    ON COALESCE(A.user_id, B.user_id, C.user_id) = D.user_id AND COALESCE(A.anime_id, B.anime_id, C.anime_id) = D.anime_id
"""

#------------- Staging Anime Anime Queries ----------
staging_anime_anime_related_query = f"""
    WITH src_anime_last_crawl_date AS (
        SELECT src_anime, MAX(crawl_date) AS last_crawl_date
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{RELATED_ANIME_ITEM_LANDING_TABLE}`
        GROUP BY src_anime
    ),
    last_crawl_related_anime AS (
        SELECT A.src_anime, A.dest_anime
        FROM `{PROJECT_ID}.{LANDING_DATASET}.{RELATED_ANIME_ITEM_LANDING_TABLE}` A
        JOIN src_anime_last_crawl_date B
        ON A.src_anime = B.src_anime AND A.crawl_date = B.last_crawl_date 
    )
    SELECT animeA, animeB
    FROM (
        (SELECT src_anime AS animeA, dest_anime AS animeB FROM last_crawl_related_anime)
        UNION DISTINCT
        (SELECT dest_anime AS animeA, src_anime AS animeB FROM last_crawl_related_anime)
    )
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

staging_anime_anime_all_query = f"""
    WITH 
    recommendation AS (
        SELECT *, 1 AS recommendation FROM `{PROJECT_ID}.{STAGING_DATASET}.{RECOMMENDED_STAGING_TABLE}`
    ),
    related AS (
        SELECT *, 1 AS related FROM `{PROJECT_ID}.{STAGING_DATASET}.{RELATED_STAGING_TABLE}`
    )
    SELECT 
        COALESCE(A.animeA, B.animeA) AS animeA,
        COALESCE(A.animeB, B.animeB) AS animeB,
        COALESCE(A.recommendation, 0) AS recommendation,
        A.recommendation_url,
        A.num_recommenders,
        COALESCE(B.related, 0) AS related
    FROM recommendation A
    FULL OUTER JOIN related B
    ON A.animeA = B.animeA AND A.animeB = B.animeB
"""
