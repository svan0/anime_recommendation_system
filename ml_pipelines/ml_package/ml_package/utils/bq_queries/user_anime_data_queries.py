from ml_package.utils.bq_queries.common_data_queries import anime_list_query, user_list_query

def user_anime_filter_anime_filter_users_query(
    user_anime_relation = "`anime-rec-dev.processed_area.user_anime`",
    anime_relation = "list_anime",
    user_relation = "list_user"
):
    query = f"""
        SELECT A.*
        FROM {user_anime_relation} A
        INNER JOIN {anime_relation} B
        ON A.anime_id = B.anime_id
        INNER JOIN {user_relation} C
        ON A.user_id = C.user_id
    """
    return query

def user_anime_completed_and_strict_ordered_query(
    user_anime_relation = "`anime-rec-dev.processed_area.user_anime`"
):
    query = f"""
        SELECT user_id, anime_id, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY last_interaction_date DESC) AS user_anime_order
        FROM {user_anime_relation}
        WHERE status = 'completed' AND last_interaction_date IS NOT NULL
    """
    return query

def user_anime_completed_and_scored_and_strict_ordered_query(
    user_anime_relation = "`anime-rec-dev.processed_area.user_anime`"
):
    query = f"""
        SELECT user_id, anime_id, score, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY last_interaction_date DESC) AS user_anime_order
        FROM {user_anime_relation}
        WHERE status = 'completed' AND score IS NOT NULL AND last_interaction_date IS NOT NULL
    """
    return query

def user_anime_completed_and_not_strict_ordered_query(
    user_anime_relation = "`anime-rec-dev.processed_area.user_anime`"
):
    query = f"""
        SELECT user_id, anime_id, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY (last_interaction_date IS NOT NULL) DESC, last_interaction_date DESC) AS user_anime_order
        FROM {user_anime_relation}
        WHERE status = 'completed'
    """
    return query

def user_anime_completed_and_scored_and_not_strict_ordered_query(
    user_anime_relation = "`anime-rec-dev.processed_area.user_anime`"
):
    query = f"""
        SELECT user_id, anime_id, score, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY (last_interaction_date IS NOT NULL) DESC, last_interaction_date DESC) AS user_anime_order
        FROM {user_anime_relation}
        WHERE status = 'completed' AND score IS NOT NULL
    """
    return query

def user_retrieved_animes_query(
    user_retrieved_anime_relation="user_retrieved_anime_table",
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
        SELECT A.user_id, A.retrieved_anime_id
        FROM {user_retrieved_anime_relation} A
        LEFT JOIN filtered_user_anime B
        ON A.user_id = B.user_id AND A.retrieved_anime_id = B.anime_id
        WHERE B.status IS NULL
    """
    return retrieved_anime_query

def user_all_possible_animes_query(
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
        ),
        user_anime_cross AS (
            SELECT A.user_id, B.anime_id AS retrieved_anime_id
            FROM list_users A
            CROSS JOIN list_anime B
        )
        SELECT A.user_id, A.retrieved_anime_id
        FROM user_anime_cross A
        LEFT JOIN filtered_user_anime B
        ON A.user_id = B.user_id AND A.retrieved_anime_id = B.anime_id
        WHERE B.status IS NULL
    """
    return retrieved_anime_query


