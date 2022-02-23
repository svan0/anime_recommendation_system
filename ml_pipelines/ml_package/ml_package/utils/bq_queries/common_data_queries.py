
def anime_list_query(
    user_anime_relation = "`anime-rec-dev.processed_area.user_anime`",
    anime_min_completed_and_rated = 0
):
    query = f"""
        SELECT anime_id
        FROM {user_anime_relation}
        WHERE status = 'completed' AND score IS NOT NULL
        GROUP BY anime_id
        HAVING COUNT(*) >= {anime_min_completed_and_rated}
    """
    return query

def user_list_query(
    user_anime_relation = "`anime-rec-dev.processed_area.user_anime`",
    user_min_completed_and_rated = 0
):
    query = f"""
        SELECT user_id
        FROM {user_anime_relation}
        WHERE status = 'completed' AND score IS NOT NULL
        GROUP BY user_id
        HAVING COUNT(*) >= {user_min_completed_and_rated}
    """
    return query
