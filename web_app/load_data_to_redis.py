import os
import logging
from google.cloud import bigquery
import redis
from tqdm import tqdm

REDIS_INSTANCE_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
REDIS_INSTANCE_PORT = 6379

ANIME_INFO_QUERY = f"""
    SELECT anime_id, title AS name, anime_url AS url, main_pic AS img_url 
    FROM `anime-rec-dev.processed_area.anime`
    ORDER BY anime_id
"""

AMIME_ANIME_USER_LAST_WATCHED = f"""
    SELECT user_id, anime_id AS recent_watch
    FROM `anime-rec-dev.ml_anime_anime.user_last_watched`
    ORDER BY user_id
"""

ANIME_ANIME_RECS_QUERY = f"""
    WITH ranked_recs AS (
        SELECT user_id, anime_id, score, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY score DESC) AS rnk
        FROM `anime-rec-dev.ml_anime_anime.user_anime_ranked`
    )
    SELECT user_id, anime_id
    FROM ranked_recs
    WHERE rnk <= 20
    ORDER BY user_id, rnk
"""

USER_ANIME_RECS_QUERY = f"""
    WITH ranked_recs AS (
        SELECT user_id, anime_id, score, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY score DESC) AS rnk
        FROM `anime-rec-dev.ml_user_anime.list_ranking_infer_filter`
    )
    SELECT user_id, anime_id
    FROM ranked_recs
    WHERE rnk <= 20
    ORDER BY user_id, rnk
"""

def load_big_query_data(query):
    client = bigquery.Client(project="anime-rec-dev")
    job_config = bigquery.QueryJobConfig()
    query_job = client.query(query, job_config=job_config)
    return query_job.result()

if __name__ == '__main__':
    logging.basicConfig(
        format='%(levelname)s: %(asctime)s: %(message)s',
        level=logging.INFO
    )
    
    logging.info("Connecting and flushng redis db")

    r = redis.Redis(
        host=REDIS_INSTANCE_HOST,
        port=REDIS_INSTANCE_PORT,
        ssl=False
    )
    r.flushdb()

    """
        Load anime info
    """
    logging.info("Loading and uploading anime info")

    anime_info = load_big_query_data(ANIME_INFO_QUERY)

    for row in tqdm(anime_info):
        r.set(f"{row['anime_id']}_name", row['name'])
        r.set(f"{row['anime_id']}_url", row['url'])
        r.set(f"{row['anime_id']}_img_url", row['img_url'])
    
    logging.info(f"1_name : {r.get('1_name')}")
    logging.info(f"1_url : {r.get('1_url')}")
    logging.info(f"1_img_url : {r.get('1_img_url')}")

    """
        Load user last watched anime
    """
    logging.info("Loading and uploading user last watched")

    user_last_watched = load_big_query_data(AMIME_ANIME_USER_LAST_WATCHED)

    for row in tqdm(user_last_watched):
        r.set(f"{row['user_id']}_recent_watch", row['recent_watch'])
    logging.info(f"---Adina---_recent_watch : {r.get('---adina---_recent_watch')}")

    """
        Load anime anime recommendations
    """
    logging.info("Loading and uploading anime anime recs")

    anime_anime = load_big_query_data(ANIME_ANIME_RECS_QUERY)

    for row in tqdm(anime_anime):
        r.rpush(f"{row['user_id']}_anime_anime_recs", row['anime_id'])
    logging.info(f"---Adina---_anime_anime_recs : {r.lrange('---adina---_anime_anime_recs', 0, -1)}")

    """
        Load user anime recommendations
    """
    logging.info("Loading and uploading user anime recs")

    user_anime = load_big_query_data(USER_ANIME_RECS_QUERY)

    for row in tqdm(user_anime):
        r.rpush(f"{row['user_id']}_user_anime_recs", row['anime_id'])
    logging.info(f"---Adina---_user_anime_recs : {r.lrange('---adina---_user_anime_recs', 0, -1)}")
