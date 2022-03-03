'''
    SQL queries used in the pipeline that recommends animes
    based on the what the user watched (collaborative filtering)
'''
from anime_rec.data.bq_queries.common_data_queries import anime_list_query, user_list_sub_query
from anime_rec.data.bq_queries.common_data_queries import user_anime_filter_anime
from anime_rec.data.bq_queries.user_anime_data_queries import user_anime_filter_user
from anime_rec.data.bq_queries.user_anime_data_queries import user_anime_completed_and_not_strict_ordered_query
from anime_rec.data.bq_queries.user_anime_data_queries import user_anime_completed_and_scored_and_not_strict_ordered_query

def user_anime_retrieval_query(
    mode='TRAIN',
    anime_min_completed_and_rated=1000,
    user_min_completed_and_rated=50
):
    '''
        SQL query that returns train, validation or test data
        for the collaborative filtering retrieval model
        Returns pairs of user / anime where the user completed the anime
        Test data are the last 10 completed animes per user
        Val data are the last 10 completed animes previous to the test data
        Train data are the other completed animes per user
    '''
    query = f"""
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
    filtered_ordered_user_anime AS (
        {user_anime_completed_and_not_strict_ordered_query("filtered_user_anime")}
    )
    """
    if mode == 'TRAIN':
        query += """
        SELECT user_id, anime_id 
        FROM filtered_ordered_user_anime 
        WHERE user_anime_order >= 21
        """
    elif mode == 'VAL':
        query += """
        SELECT user_id, anime_id 
        FROM filtered_ordered_user_anime 
        WHERE user_anime_order BETWEEN 11 AND 20
        """
    else:
        query += """
        SELECT user_id, anime_id 
        FROM filtered_ordered_user_anime 
        WHERE user_anime_order BETWEEN 1 AND 10
        """
    return query

def user_anime_ranking_query(
    mode='TRAIN',
    anime_min_completed_and_rated=1000,
    user_min_completed_and_rated=50
):
    '''
        SQL query that returns train, validation or test data
        for the collaborative filtering list ranking model
        For each user we return a list of 10 scored animes
        and a list of the score
        The model will need to learn to rank those lists of animes
    '''
    query = f"""
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
    filtered_ordered_user_anime AS (
        {user_anime_completed_and_scored_and_not_strict_ordered_query("filtered_user_anime")}
    )
    """
    if mode == 'TRAIN':
        query += """
        SELECT user_id, anime_id, score
        FROM filtered_ordered_user_anime
        WHERE user_anime_order >= 21
        """
    elif mode == 'VAL':
        query += """
        SELECT user_id, anime_id, score
        FROM filtered_ordered_user_anime
        WHERE user_anime_order BETWEEN 11 AND 20
        """
    else:
        query += """
        SELECT user_id, anime_id, score
        FROM filtered_ordered_user_anime
        WHERE user_anime_order BETWEEN 1 AND 10
        """
    return query

def user_anime_list_ranking_query(
    mode='TRAIN',
    anime_min_completed_and_rated=1000,
    user_min_completed_and_rated=50
):
    '''
        SQL query that returns train, validation or test data
        for the collaborative filtering list ranking model
        For each user we return a list of 10 scored animes
        and a list of the score
        The model will need to learn to rank those lists of animes
    '''
    query = f"""
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
    filtered_ordered_user_anime AS (
        {user_anime_completed_and_scored_and_not_strict_ordered_query("filtered_user_anime")}
    ),
    train_data AS (
        SELECT user_id, anime_id, score
        FROM filtered_ordered_user_anime
        WHERE user_anime_order >= 21
    ),
    train_data_random_order AS (
        SELECT user_id, 
               anime_id, 
               CAST(score AS STRING) AS score, 
               ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY RAND()) - 1 AS random_order_anime_per_user
        FROM train_data
    ),
    train_data_list AS (
        SELECT user_id, 
               ARRAY_AGG(anime_id) AS anime_id, 
               ARRAY_AGG(score) AS score 
        FROM train_data_random_order
        GROUP BY user_id, DIV(random_order_anime_per_user, 10)
        HAVING ARRAY_LENGTH(anime_id) = 10
    ),
    val_data AS (
        SELECT user_id, anime_id, CAST(score AS STRING) AS score
        FROM filtered_ordered_user_anime
        WHERE user_anime_order BETWEEN 11 AND 20
    ),
    val_data_list AS (
        SELECT user_id, 
               ARRAY_AGG(anime_id) AS anime_id, 
               ARRAY_AGG(score) AS score 
        FROM val_data 
        GROUP BY user_id
    ),
    test_data AS (
        SELECT user_id, anime_id, CAST(score AS STRING) AS score
        FROM filtered_ordered_user_anime
        WHERE user_anime_order BETWEEN 1 AND 10
    ),
    test_data_list AS (
        SELECT user_id, 
               ARRAY_AGG(anime_id) AS anime_id, 
               ARRAY_AGG(score) AS score 
        FROM test_data 
        GROUP BY user_id
    )
    """
    if mode == 'TRAIN':
        query += """
        SELECT user_id, ARRAY_TO_STRING(anime_id, '|') AS anime_id, ARRAY_TO_STRING(score, '|') AS score
        FROM train_data_list
        """
    elif mode == 'VAL':
        query += """
        SELECT user_id, ARRAY_TO_STRING(anime_id, '|') AS anime_id, ARRAY_TO_STRING(score, '|') AS score
        FROM val_data_list
        """
    else:
        query += """
        SELECT user_id, ARRAY_TO_STRING(anime_id, '|') AS anime_id, ARRAY_TO_STRING(score, '|') AS score
        FROM test_data_list
        """
    return query

