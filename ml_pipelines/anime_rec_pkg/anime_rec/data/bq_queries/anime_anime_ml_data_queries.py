'''
    SQL queries used in the pipeline that recommends animes
    based on the last anime the user watched
'''
from anime_rec.data.bq_queries.anime_anime_data_queries import anime_anime_co_occurance_query, anime_anime_recommended_query, anime_anime_related_query
from anime_rec.data.bq_queries.anime_anime_data_queries import anime_anime_all_query, anime_anime_all_order_query
from anime_rec.data.bq_queries.user_anime_data_queries import user_anime_filter_user
from anime_rec.data.bq_queries.user_anime_data_queries import user_anime_completed_and_strict_ordered_query 
from anime_rec.data.bq_queries.user_anime_data_queries import user_anime_completed_and_scored_and_strict_ordered_query
from anime_rec.data.bq_queries.common_data_queries import user_anime_filter_anime
from anime_rec.data.bq_queries.common_data_queries import anime_list_query, user_list_sub_query


def anime_anime_retrieval_query(
        mode='TRAIN',
        anime_min_completed_and_rated=1000,
        user_min_completed_and_rated=50,
        max_co_completed_distance=10,
        min_co_completed_count=10,
        min_num_recommenders=5,
        max_rank=100
):
    '''
        SQL query that return train, validation or test data
        for the anime anime retrieval model
        The query returns pairs of anime that are either related,
        recommeded by `min_num_recommenders` users or
        cowatched (in a short time frame `max_co_watch_distance`)
        by at least `min_co_watched_count` users
    '''
    anime_anime_query = f"""
    WITH 
    list_anime AS (
        {anime_list_query("`anime-rec-dev.processed_area.user_anime`", anime_min_completed_and_rated)}
    ),
    filtered_user_anime_on_anime AS (
        {user_anime_filter_anime("`anime-rec-dev.processed_area.user_anime`", "list_anime")}
    ),
    list_users AS (
        {user_list_sub_query("filtered_user_anime_on_anime", user_min_completed_and_rated)}
    ),
    filtered_user_anime AS (
        {user_anime_filter_user("filtered_user_anime_on_anime", "list_users")}
    ),
    user_anime AS (
        {user_anime_completed_and_strict_ordered_query("filtered_user_anime")}
    ),
    anime_co_completed_anime AS (
        {anime_anime_co_occurance_query('user_anime', max_co_completed_distance, min_co_completed_count)}
    ),
    anime_related_anime AS (
        {anime_anime_related_query("`anime-rec-dev.processed_area.anime_anime`")}
    ),
    anime_recommended_anime AS (
        {anime_anime_recommended_query("`anime-rec-dev.processed_area.anime_anime`", min_num_recommenders)}
    ),
    anime_anime AS (
        {anime_anime_all_query("anime_related_anime", "anime_recommended_anime", "anime_co_completed_anime")}
    ),
    anime_anime_ordered AS (
        {anime_anime_all_order_query("anime_anime")}
    )
    """
    if mode == 'TRAIN':
        anime_anime_query += f"""
        SELECT animeA AS anime_id, animeB AS retrieved_anime_id
        FROM anime_anime_ordered 
        WHERE (animeA_rank <= {max_rank} OR animeB_rank <= {max_rank}) 
        AND animeA < animeB 
        AND ABS(MOD(FARM_FINGERPRINT(CONCAT(animeA, animeB)), 10)) BETWEEN 0 AND 7
        """
    elif mode == 'VAL':
        anime_anime_query += f"""
        SELECT animeA AS anime_id, animeB AS retrieved_anime_id
        FROM anime_anime_ordered 
        WHERE (animeA_rank <= {max_rank} OR animeB_rank <= {max_rank})
        AND animeA < animeB 
        AND ABS(MOD(FARM_FINGERPRINT(CONCAT(animeA, animeB)), 10)) = 8
        """
    else:
        anime_anime_query += f"""
        SELECT animeA AS anime_id, animeB AS retrieved_anime_id
        FROM anime_anime_ordered 
        WHERE (animeA_rank <= {max_rank} OR animeB_rank <= {max_rank}) 
        AND animeA < animeB 
        AND ABS(MOD(FARM_FINGERPRINT(CONCAT(animeA, animeB)), 10)) = 9
        """
    return anime_anime_query

def anime_anime_pair_ranking_query(
        mode='TRAIN',
        anime_min_completed_and_rated=1000,
        user_min_completed_and_rated=50,
        max_co_completed_distance=10,
        min_co_completed_count=10,
):
    '''
        SQL query that return train, validation or test data
        for the anime anime pair ranking model
        For each anchor anime we return a pair of relevant animes
        (relevant anime are selected by the retrieval query)
        We also return a label (1 or 0) based on whether the first
        relevant anime (rel_anime_1) is better ranked than the second
        relevant anime (rel_anime_2)
    '''
    anime_anime_query = f"""
    WITH 
    list_anime AS (
        {anime_list_query("`anime-rec-dev.processed_area.user_anime`", anime_min_completed_and_rated)}
    ),
    filtered_user_anime_on_anime AS (
        {user_anime_filter_anime("`anime-rec-dev.processed_area.user_anime`", "list_anime")}
    ),
    list_users AS (
        {user_list_sub_query("filtered_user_anime_on_anime", user_min_completed_and_rated)}
    ),
    filtered_user_anime AS (
        {user_anime_filter_user("filtered_user_anime_on_anime", "list_users")}
    ),
    user_anime AS (
        {user_anime_completed_and_scored_and_strict_ordered_query("filtered_user_anime")}
    ),
    anime_co_completed_anime AS (
        {anime_anime_co_occurance_query('user_anime', max_co_completed_distance, min_co_completed_count)}
    ),
    positive_pairs AS (
        SELECT A.animeA AS anime_id, A.animeB AS retrieved_anime_id_1, B.animeB AS retrieved_anime_id_2, 1 AS label
        FROM anime_co_completed_anime A
        LEFT JOIN anime_co_completed_anime B
        ON A.animeA = B.animeA
        WHERE A.avg_score_diff > B.avg_score_diff + 3.0
    ),
    negative_pairs AS (
        SELECT A.animeA AS anime_id, A.animeB AS retrieved_anime_id_1, B.animeB AS retrieved_anime_id_2, 0 AS label
        FROM anime_co_completed_anime A
        LEFT JOIN anime_co_completed_anime B
        ON A.animeA = B.animeA
        WHERE B.avg_score_diff > A.avg_score_diff + 3.0
    ),
    all_pairs AS (
        SELECT anime_id, retrieved_anime_id_1, retrieved_anime_id_2, label FROM positive_pairs
        UNION DISTINCT
        SELECT anime_id, retrieved_anime_id_1, retrieved_anime_id_2, label FROM negative_pairs
    )
    """
    if mode == 'TRAIN':
        anime_anime_query += """
        SELECT *
        FROM all_pairs 
        WHERE ABS(MOD(FARM_FINGERPRINT(CONCAT(anime_id, retrieved_anime_id_1)), 10)) BETWEEN 0 AND 7
        """
    elif mode == 'VAL':
        anime_anime_query += """
        SELECT *
        FROM all_pairs 
        WHERE ABS(MOD(FARM_FINGERPRINT(CONCAT(anime_id, retrieved_anime_id_1)), 10)) = 8
        """
    else:
        anime_anime_query += """
        SELECT *
        FROM all_pairs 
        WHERE ABS(MOD(FARM_FINGERPRINT(CONCAT(anime_id, retrieved_anime_id_1)), 10)) = 9
        """
    return anime_anime_query
