from anime_rec.data.bq_queries.common_data_queries import user_anime_filter_anime
from anime_rec.data.bq_queries.user_anime_data_queries import user_anime_completed_and_strict_ordered_query
from anime_rec.data.bq_queries.common_data_queries import anime_list_query, filter_recommendations


def anime_anime_related_query(anime_anime_relation = "`anime-rec-dev.processed_area.anime_anime`"):
    query = f"""
        SELECT animeA, animeB, related
        FROM {anime_anime_relation}
        WHERE related = 1
    """
    return query

def anime_anime_recommended_query(
    anime_anime_relation = "`anime-rec-dev.processed_area.anime_anime`"
):
    query = f"""
        SELECT animeA, animeB, num_recommenders, NTILE(5) OVER (PARTITION BY animeA ORDER BY num_recommenders ASC) AS bucket_num_recommenders
        FROM {anime_anime_relation} 
        WHERE recommendation = 1
    """
    return query

def anime_anime_co_occurance_query(
    user_anime_relation = 'user_anime',
    max_co_occurance_distance = 10
):
    query = f"""
        SELECT animeA, animeB, co_occurence_cnt, NTILE(5) OVER (PARTITION BY animeA ORDER BY co_occurence_cnt ASC) AS bucket_co_occurence_cnt
        FROM (
            SELECT A.anime_id AS animeA, B.anime_id AS animeB, COUNT(*) AS co_occurence_cnt
            FROM {user_anime_relation} A 
            LEFT JOIN {user_anime_relation} B
            ON A.user_id = B.user_id 
            WHERE (ABS(A.user_anime_order - B.user_anime_order) BETWEEN 1 AND {max_co_occurance_distance})
            GROUP BY A.anime_id, B.anime_id
        )
    """
    return query

def anime_anime_genres_cosine_query(
    anime_info_relation = 'anime_info'
):
    query = f"""
        SELECT animeA, animeB, cosine_sim, NTILE(5) OVER (PARTITION BY animeA ORDER BY cosine_sim ASC) AS bucket_cosine_sim
        FROM (
            SELECT A.anime_id AS animeA, B.anime_id AS animeB, ARRAY_LENGTH(functions.array_intersect(A.genres, B.genres)) / (POW(ARRAY_LENGTH(A.genres), 2) + POW(ARRAY_LENGTH(B.genres), 2)) AS cosine_sim
            FROM {anime_info_relation} A
            CROSS JOIN {anime_info_relation} B
        )
    """
    return query

def anime_anime_all_query(
    anime_anime_related_relation = 'anime_related_anime',
    anime_anime_recommended_relation = 'anime_recommended_anime',
    anime_anime_co_completed_relation = 'anime_co_completed_anime',
    anime_anime_cosine_sim_relation = 'anime_cosine_anime'
):
    query = f"""
        SELECT  A.animeA AS animeA, 
                A.animeB AS animeB, 
                COALESCE(A.co_occurence_cnt, 0) AS co_occurence_cnt, 
                COALESCE(B.related, 0) AS related, 
                COALESCE(C.num_recommenders, 0) AS num_recommenders,
                COALESCE(D.cosine_sim, 0) AS cosine_sim,
                COALESCE(A.bucket_co_occurence_cnt, 0) AS bucket_co_occurence_cnt, 
                COALESCE(C.bucket_num_recommenders, 0) AS bucket_num_recommenders,
                COALESCE(D.bucket_cosine_sim, 0) AS bucket_cosine_sim
        FROM {anime_anime_co_completed_relation} A
        FULL OUTER JOIN {anime_anime_related_relation} B
        ON A.animeA = B.animeA AND A.animeB = B.animeB
        FULL OUTER JOIN {anime_anime_recommended_relation} C
        ON A.animeA = C.animeA AND A.animeB = C.animeB    
        FULL OUTER JOIN {anime_anime_cosine_sim_relation} D
        ON A.animeA = D.animeA AND A.animeB = D.animeB 
    """
    return query

def anime_anime_all_order_query(anime_anime_relation = 'anime_anime'):
    query = f"""
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY animeA ORDER BY related DESC, bucket_cosine_sim DESC, bucket_num_recommenders DESC, bucket_co_occurence_cnt DESC) AS animeB_rank,
            ROW_NUMBER() OVER (PARTITION BY animeB ORDER BY related DESC, bucket_cosine_sim DESC, bucket_num_recommenders DESC, bucket_co_occurence_cnt DESC) AS animeA_rank
        FROM {anime_anime_relation}
    """
    return query


def anime_all_possible_anime_query(anime_min_completed_and_rated=1000):
    anime_cross_anime = f"""
        WITH 
        list_anime AS (
            {anime_list_query("`anime-rec-dev.processed_area.user_anime`", anime_min_completed_and_rated)}
        )
        SELECT A.anime_id, B.anime_id AS retrieved_anime_id
        FROM list_anime A
        CROSS JOIN list_anime B
    """
    return anime_cross_anime


def user_last_anime_watched_query(
    anime_min_completed_and_rated=1000
):
    '''
        SQL query that returns for each user the last anime they completed
    '''
    last_watched_query = f"""
        WITH 
        list_anime AS (
            {anime_list_query("`anime-rec-dev.processed_area.user_anime`", anime_min_completed_and_rated)}
        ),
        filtered_user_anime_on_anime AS (
            {user_anime_filter_anime("`anime-rec-dev.processed_area.user_anime`", "list_anime")}
        ),
        user_anime AS (
            {user_anime_completed_and_strict_ordered_query("filtered_user_anime_on_anime")} AND score IS NOT NULL AND score > 5
        )
        SELECT user_id, anime_id FROM user_anime WHERE user_anime_order = 1
    """
    return last_watched_query

def user_last_anime_ranked_animes_query(
    user_last_watched_relation="user_last_anime_watched_table",
    anime_anime_scored_relation="anime_anime_scored_table"
):
    '''
        SQL queries that takes all the ranked animes for each anime
        and each user's last wacthed anime
        and returns ranked anime for each user 
    '''
    user_anime_scored_query = f"""
        WITH 
        initial_recommendations AS (
            SELECT A.user_id, B.retrieved_anime_id AS anime_id, B.score
            FROM {user_last_watched_relation} A
            LEFT JOIN {anime_anime_scored_relation} B
            ON A.anime_id = B.anime_id
        )
        {filter_recommendations("initial_recommendations")}
    """
    return user_anime_scored_query