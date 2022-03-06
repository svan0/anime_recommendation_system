def anime_list_query(
    user_anime_relation = "`anime-rec-dev.processed_area.user_anime`",
    anime_min_completed_and_rated = 1000
):
    query = f"""
        SELECT anime_id
        FROM {user_anime_relation}
        WHERE status = 'completed' AND score IS NOT NULL
        GROUP BY anime_id
        HAVING COUNT(*) >= {anime_min_completed_and_rated}
    """
    return query

def user_anime_filter_anime(
    user_anime_relation = "`anime-rec-dev.processed_area.user_anime`",
    anime_relation = "list_anime"
):
    query = f"""
        SELECT A.*
        FROM {user_anime_relation} A
        INNER JOIN {anime_relation} B
        ON A.anime_id = B.anime_id
    """
    return query

def user_list_query(
    user_anime_relation = "`anime-rec-dev.processed_area.user_anime`",
    anime_min_completed_and_rated = 1000,
    user_min_completed_and_rated = 50
):
    query = f"""
        WITH 
        list_anime AS (
            {anime_list_query(user_anime_relation, anime_min_completed_and_rated)}
        ),
        filtered_user_anime_on_anime AS (
            {user_anime_filter_anime("`anime-rec-dev.processed_area.user_anime`", "list_anime")}
        )
        SELECT user_id
        FROM filtered_user_anime_on_anime
        WHERE status = 'completed' AND score IS NOT NULL
        GROUP BY user_id
        HAVING COUNT(*) >= {user_min_completed_and_rated}
    """
    return query

def user_list_sub_query(
    user_anime_relation = "`anime-rec-dev.processed_area.user_anime`",
    user_min_completed_and_rated = 50
):
    query = f"""
        SELECT user_id
        FROM {user_anime_relation}
        WHERE status = 'completed' AND score IS NOT NULL
        GROUP BY user_id
        HAVING COUNT(*) >= {user_min_completed_and_rated}
    """
    return query


