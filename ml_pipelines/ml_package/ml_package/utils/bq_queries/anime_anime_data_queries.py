from ml_package.utils.bq_queries.user_anime_data_queries import user_anime_filter_anime_filter_users_query, user_anime_completed_and_strict_ordered_query
from ml_package.utils.bq_queries.common_data_queries import anime_list_query, user_list_query


def anime_anime_related_query(anime_anime_relation = "`anime-rec-dev.processed_area.anime_anime`"):
    query = f"""
        SELECT animeA, animeB, related
        FROM {anime_anime_relation}
        WHERE related = 1
    """
    return query

def anime_anime_recommended_query(
    anime_anime_relation = "`anime-rec-dev.processed_area.anime_anime`",
    min_num_recommenders = 5
):
    query = f"""
        SELECT animeA, animeB, num_recommenders
        FROM {anime_anime_relation} 
        WHERE recommendation = 1 AND num_recommenders >= {min_num_recommenders}
    """
    return query

def anime_anime_co_occurance_query(
    user_anime_relation = 'user_anime',
    max_co_occurance_distance = 10,
    min_co_occurance_count = 10
):
    query = f"""
        SELECT A.anime_id AS animeA, B.anime_id AS animeB, COUNT(*) AS co_occurence_cnt, AVG(ABS(A.score - B.score)) AS avg_score_diff
        FROM {user_anime_relation} A 
        LEFT JOIN {user_anime_relation} B
        ON A.user_id = B.user_id 
        WHERE (ABS(A.user_anime_order - B.user_anime_order) BETWEEN 1 AND {max_co_occurance_distance})
        GROUP BY A.anime_id, B.anime_id
        HAVING co_occurence_cnt >= {min_co_occurance_count}
    """
    return query

def anime_anime_all_query(
    anime_anime_related_relation = 'anime_related_anime',
    anime_anime_recommended_relation = 'anime_recommended_anime',
    anime_anime_co_completed_relation = 'anime_co_completed_anime'
):
    query = f"""
        SELECT  A.animeA AS animeA, 
                A.animeB AS animeB, 
                COALESCE(A.co_occurence_cnt, 0) AS co_occurence_cnt, 
                COALESCE(B.related, 0) AS related, 
                COALESCE(C.num_recommenders, 0) AS num_recommenders
        FROM {anime_anime_co_completed_relation} A
        FULL OUTER JOIN {anime_anime_related_relation} B
        ON A.animeA = B.animeA AND A.animeB = B.animeB
        FULL OUTER JOIN {anime_anime_recommended_relation} C
        ON A.animeA = C.animeA AND A.animeB = C.animeB    
    """
    return query

def anime_anime_all_order_query(anime_anime_relation = 'anime_anime'):
    query = f"""
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY animeA ORDER BY related DESC, num_recommenders DESC, co_occurence_cnt DESC) AS animeB_rank,
            ROW_NUMBER() OVER (PARTITION BY animeB ORDER BY related DESC, num_recommenders DESC, co_occurence_cnt DESC) AS animeA_rank
        FROM {anime_anime_relation}
    """
    return query

def user_last_anime_watched_query(
    anime_min_completed_and_rated=1000,
    users_min_completed_and_rated=50,
):
    '''
        SQL query that returns for each user the last anime they completed
    '''
    last_watched_query = f"""
        WITH 
        list_anime AS (
            {anime_list_query("`anime-rec-dev.processed_area.user_anime`", anime_min_completed_and_rated)}
        ),
        list_users AS (
            {user_list_query("`anime-rec-dev.processed_area.user_anime`", users_min_completed_and_rated)}
        ),
        filtered_user_anime AS (
            {user_anime_filter_anime_filter_users_query("`anime-rec-dev.processed_area.user_anime`", "list_anime", "list_user")}
        ),
        user_anime AS (
            {user_anime_completed_and_strict_ordered_query("filtered_user_anime")}
        )
        SELECT user_id, anime_id FROM user_anime WHERE user_anime_order = 1
    """
    return last_watched_query

def user_last_anime_retrieved_animes_query(
    user_retrieved_anime_relation="user_last_anime_retrieved_anime_table",
    anime_min_completed_and_rated=1000,
    users_min_completed_and_rated=50
):
    '''
        SQL queries that takes all the retrieved animes for each user
        and only returns retrieved animes that haven't been seen
        by the user
    '''
    retrieved_anime_query = f"""
        WITH 
        list_anime AS (
            {anime_list_query("`anime-rec-dev.processed_area.user_anime`", anime_min_completed_and_rated)}
        ),
        list_users AS (
            {user_list_query("`anime-rec-dev.processed_area.user_anime`", users_min_completed_and_rated)}
        ),
        filtered_user_anime AS (
            {user_anime_filter_anime_filter_users_query("`anime-rec-dev.processed_area.user_anime`", "list_anime", "list_user")}
        )
        SELECT A.user_id, A.anime_id, A.retrieved_anime_id
        FROM {user_retrieved_anime_relation} A
        LEFT JOIN filtered_user_anime B
        ON A.user_id = B.user_id AND A.retrieved_anime_id = B.anime_id
        WHERE B.status IS NULL
    """
    return retrieved_anime_query