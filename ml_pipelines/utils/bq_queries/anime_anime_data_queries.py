'''
    SQL queries used in the pipeline that recommends animes
    based on the last anime the user watched
'''
def all_anime_query():
    '''
        SQL query that returns list of all unique animes
    '''
    query = '''
        SELECT DISTINCT(anime_id) FROM `anime-rec-dev.processed_area.user_anime`
    '''
    return query

def anime_anime_retrieval_query(
        mode='TRAIN',
        max_co_watch_distance=10,
        min_co_watched_count=10,
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
    user_anime AS (
        SELECT user_id, anime_id, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY last_interaction_date ASC) AS user_anime_order
        FROM `anime-rec-dev.processed_area.user_anime`
        WHERE status = 'completed' AND last_interaction_date IS NOT NULL
    ),
    anime_co_watch_anime AS (
        SELECT A.anime_id AS animeA, B.anime_id AS animeB, COUNT(*) AS co_occurence_cnt
        FROM user_anime A 
        LEFT JOIN user_anime B
        ON A.user_id = B.user_id 
        WHERE (ABS(A.user_anime_order - B.user_anime_order) BETWEEN 1 AND {max_co_watch_distance})
        GROUP BY A.anime_id, B.anime_id
        HAVING co_occurence_cnt >= {min_co_watched_count}
    ),
    anime_related_anime AS (
        SELECT animeA, animeB, related
        FROM `anime-rec-dev.processed_area.anime_anime` 
        WHERE related = 1
    ),
    anime_recommended_anime AS (
        SELECT animeA, animeB, num_recommenders
        FROM `anime-rec-dev.processed_area.anime_anime` 
        WHERE recommendation = 1 AND num_recommenders >= {min_num_recommenders}
    ),
    anime_anime AS (
        SELECT  A.animeA AS animeA, 
                A.animeB AS animeB, 
                COALESCE(A.co_occurence_cnt, 0) AS co_occurence_cnt, 
                COALESCE(B.related, 0) AS related, 
                COALESCE(C.num_recommenders, 0) AS num_recommenders
        FROM anime_co_watch_anime A
        FULL OUTER JOIN anime_related_anime B
        ON A.animeA = B.animeA AND A.animeB = B.animeB
        FULL OUTER JOIN anime_recommended_anime C
        ON A.animeA = C.animeA AND A.animeB = C.animeB
    ),
    anime_anime_ordered AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY animeA ORDER BY related DESC, num_recommenders DESC, co_occurence_cnt DESC) AS animeB_rank,
            ROW_NUMBER() OVER (PARTITION BY animeB ORDER BY related DESC, num_recommenders DESC, co_occurence_cnt DESC) AS animeA_rank
        FROM anime_anime
    )
    """
    if mode == 'TRAIN':
        anime_anime_query += f"""
        SELECT animeA, animeB 
        FROM anime_anime_ordered 
        WHERE (animeA_rank <= {max_rank} OR animeB_rank <= {max_rank}) 
        AND animeA < animeB 
        AND ABS(MOD(FARM_FINGERPRINT(CONCAT(animeA, animeB)), 10)) BETWEEN 0 AND 7
        """
    elif mode == 'VAL':
        anime_anime_query += f"""
        SELECT animeA, animeB 
        FROM anime_anime_ordered 
        WHERE (animeA_rank <= {max_rank} OR animeB_rank <= {max_rank})
        AND animeA < animeB 
        AND ABS(MOD(FARM_FINGERPRINT(CONCAT(animeA, animeB)), 10)) = 8
        """
    else:
        anime_anime_query += f"""
        SELECT animeA, animeB 
        FROM anime_anime_ordered 
        WHERE (animeA_rank <= {max_rank} OR animeB_rank <= {max_rank}) 
        AND animeA < animeB 
        AND ABS(MOD(FARM_FINGERPRINT(CONCAT(animeA, animeB)), 10)) = 9
        """
    return anime_anime_query

def anime_anime_pair_ranking_query(
        mode='TRAIN',
        max_co_watch_distance=10,
        min_co_watched_count=10,
        min_num_recommenders=5,
        max_rank=100
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
    user_anime AS (
        SELECT user_id, anime_id, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY last_interaction_date ASC) AS user_anime_order
        FROM `anime-rec-dev.processed_area.user_anime`
        WHERE status = 'completed' AND last_interaction_date IS NOT NULL
    ),
    anime_co_watch_anime AS (
        SELECT A.anime_id AS animeA, B.anime_id AS animeB, COUNT(*) AS co_occurence_cnt
        FROM user_anime A 
        LEFT JOIN user_anime B
        ON A.user_id = B.user_id 
        WHERE (ABS(A.user_anime_order - B.user_anime_order) BETWEEN 1 AND {max_co_watch_distance})
        GROUP BY A.anime_id, B.anime_id
        HAVING co_occurence_cnt >= {min_co_watched_count}
    ),
    anime_related_anime AS (
        SELECT animeA, animeB, related
        FROM `anime-rec-dev.processed_area.anime_anime` 
        WHERE related = 1
    ),
    anime_recommended_anime AS (
        SELECT animeA, animeB, num_recommenders
        FROM `anime-rec-dev.processed_area.anime_anime` 
        WHERE recommendation = 1 AND num_recommenders >= {min_num_recommenders}
    ),
    anime_anime AS (
        SELECT  A.animeA AS animeA, 
                A.animeB AS animeB, 
                COALESCE(A.co_occurence_cnt, 0) AS co_occurence_cnt, 
                COALESCE(B.related, 0) AS related, 
                COALESCE(C.num_recommenders, 0) AS num_recommenders
        FROM anime_co_watch_anime A
        FULL OUTER JOIN anime_related_anime B
        ON A.animeA = B.animeA AND A.animeB = B.animeB
        FULL OUTER JOIN anime_recommended_anime C
        ON A.animeA = C.animeA AND A.animeB = C.animeB
    ),
    ordered_anime_anime AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY animeA ORDER BY related DESC, num_recommenders DESC, co_occurence_cnt DESC) AS animeB_rank
        FROM anime_anime
    ),
    positive_pairs AS (
        SELECT A.animeA AS anchor_anime, A.animeB AS rel_anime_1, B.animeB AS rel_anime_2, 1 AS label
        FROM ordered_anime_anime A
        LEFT JOIN ordered_anime_anime B
        ON A.animeA = B.animeA AND A.animeB_rank + 10 < B.animeB_rank
        WHERE A.animeB_rank <= {max_rank} AND B.animeB_rank <= {max_rank}
    ),
    negative_pairs AS (
        SELECT A.animeA AS anchor_anime, A.animeB AS rel_anime_1, B.animeB AS rel_anime_2, 0 AS label
        FROM ordered_anime_anime A
        LEFT JOIN ordered_anime_anime B
        ON A.animeA = B.animeA AND A.animeB_rank > B.animeB_rank + 10
        WHERE A.animeB_rank <= {max_rank} AND B.animeB_rank <= {max_rank}
    ),
    all_pairs AS (
        SELECT anchor_anime, rel_anime_1, rel_anime_2, label FROM positive_pairs
        UNION DISTINCT
        SELECT anchor_anime, rel_anime_1, rel_anime_2, label FROM negative_pairs
    )
    """
    if mode == 'TRAIN':
        anime_anime_query += """
        SELECT *
        FROM all_pairs 
        WHERE ABS(MOD(FARM_FINGERPRINT(CONCAT(anchor_anime, rel_anime_1)), 10)) BETWEEN 0 AND 7
        """
    elif mode == 'VAL':
        anime_anime_query += """
        SELECT *
        FROM all_pairs 
        WHERE ABS(MOD(FARM_FINGERPRINT(CONCAT(anchor_anime, rel_anime_1)), 10)) = 8
        """
    else:
        anime_anime_query += """
        SELECT *
        FROM all_pairs 
        WHERE ABS(MOD(FARM_FINGERPRINT(CONCAT(anchor_anime, rel_anime_1)), 10)) = 9
        """
    return anime_anime_query

def user_last_anime_watched():
    '''
        SQL query that returns for each user the last anime they enjoyed watching
    '''
    last_watched_query = """
        WITH user_anime_watch AS (
            SELECT user_id, anime_id, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY last_interaction_date DESC) AS user_anime_order
            FROM `anime-rec-dev.processed_area.user_anime`
            WHERE status = 'completed' 
            AND last_interaction_date IS NOT NULL
            AND (score IS NULL OR score > 5)
        )
        SELECT user_id, anime_id FROM user_anime_watch WHERE user_anime_order = 1
    """
    return last_watched_query

def user_last_anime_retrieved_animes(retrieved_data_name):
    '''
        SQL queries that takes all the retrieved animes for each user
        and only returns retrieved animes that haven't been seen
        by the user
    '''
    retrieved_anime_query = f"""
        SELECT A.user_id, A.anime_id, A.retrieved_anime_id
        FROM {retrieved_data_name} A
        LEFT JOIN `anime-rec-dev.processed_area.user_anime` B
        ON A.user_id = B.user_id AND A.retrieved_anime_id = B.anime_id
        WHERE B.status IS NULL
    """
    return retrieved_anime_query

'''
    Similar queries but on a sample of 100 animes
'''
def sample_all_anime_query():
    query = """
    WITH
    list_anime AS (
        SELECT anime_id, COUNT(*) AS cnt
        FROM `anime-rec-dev.processed_area.user_anime`
        WHERE status = 'completed'
        GROUP BY anime_id
        ORDER BY cnt DESC, anime_id
        LIMIT 100
    )
    SELECT anime_id FROM list_anime
    """
    return query

def sample_anime_anime_retrieval_query(mode='TRAIN'):
    anime_anime_query = f"""
    WITH 
    list_anime AS (
        SELECT anime_id, COUNT(*) AS cnt
        FROM `anime-rec-dev.processed_area.user_anime`
        WHERE status = 'completed'
        GROUP BY anime_id
        ORDER BY cnt DESC
        LIMIT 100
    ),
    user_anime AS (
        SELECT A.user_id, A.anime_id, ROW_NUMBER() OVER (PARTITION BY A.user_id ORDER BY A.last_interaction_date ASC) AS user_anime_order
        FROM `anime-rec-dev.processed_area.user_anime` A
        INNER JOIN list_anime B
        ON A.anime_id = B.anime_id
        WHERE A.status = 'completed' AND A.last_interaction_date IS NOT NULL
    ),
    anime_co_watch_anime AS (
        SELECT A.anime_id AS animeA, B.anime_id AS animeB, COUNT(*) AS co_occurence_cnt
        FROM user_anime A 
        LEFT JOIN user_anime B
        ON A.user_id = B.user_id 
        WHERE (ABS(A.user_anime_order - B.user_anime_order) BETWEEN 1 AND 10)
        GROUP BY A.anime_id, B.anime_id
        HAVING co_occurence_cnt >= 10
    ),
    anime_related_anime AS (
        SELECT animeA, animeB, related
        FROM `anime-rec-dev.processed_area.anime_anime` A
        INNER JOIN list_anime B
        ON A.animeA = B.anime_id
        INNER JOIN list_anime C
        ON A.animeB = C.anime_id
        WHERE A.related = 1
    ),
    anime_recommended_anime AS (
        SELECT animeA, animeB, num_recommenders
        FROM `anime-rec-dev.processed_area.anime_anime` A
        INNER JOIN list_anime B
        ON A.animeA = B.anime_id
        INNER JOIN list_anime C
        ON A.animeB = C.anime_id
        WHERE A.recommendation = 1 AND A.num_recommenders >= 5
    ),
    anime_anime AS (
        SELECT  A.animeA AS animeA, 
                A.animeB AS animeB, 
                COALESCE(A.co_occurence_cnt, 0) AS co_occurence_cnt, 
                COALESCE(B.related, 0) AS related, 
                COALESCE(C.num_recommenders, 0) AS num_recommenders
        FROM anime_co_watch_anime A
        FULL OUTER JOIN anime_related_anime B
        ON A.animeA = B.animeA AND A.animeB = B.animeB
        FULL OUTER JOIN anime_recommended_anime C
        ON A.animeA = C.animeA AND A.animeB = C.animeB
    )
    """
    if mode == 'TRAIN':
        anime_anime_query += f"""
        SELECT animeA, animeB 
        FROM anime_anime 
        WHERE animeA < animeB AND 
        ABS(MOD(FARM_FINGERPRINT(CONCAT(animeA, animeB)), 10)) BETWEEN 0 AND 7
        """
    elif mode == 'VAL':
        anime_anime_query += f"""
        SELECT animeA, animeB 
        FROM anime_anime 
        WHERE animeA < animeB 
        AND ABS(MOD(FARM_FINGERPRINT(CONCAT(animeA, animeB)), 10)) = 8
        """
    else:
        anime_anime_query += f"""
        SELECT animeA, animeB 
        FROM anime_anime 
        WHERE animeA < animeB 
        AND ABS(MOD(FARM_FINGERPRINT(CONCAT(animeA, animeB)), 10)) = 9
        """
    return anime_anime_query

def sample_anime_anime_pair_ranking_query(mode='TRAIN'):
    anime_anime_query = f"""
    WITH 
    list_anime AS (
        SELECT anime_id, COUNT(*) AS cnt
        FROM `anime-rec-dev.processed_area.user_anime`
        WHERE status = 'completed'
        GROUP BY anime_id
        ORDER BY cnt DESC
        LIMIT 100
    ),
    user_anime AS (
        SELECT A.user_id, A.anime_id, ROW_NUMBER() OVER (PARTITION BY A.user_id ORDER BY A.last_interaction_date ASC) AS user_anime_order
        FROM `anime-rec-dev.processed_area.user_anime` A
        INNER JOIN list_anime B
        ON A.anime_id = B.anime_id
        WHERE A.status = 'completed' AND A.last_interaction_date IS NOT NULL
    ),
    anime_co_watch_anime AS (
        SELECT A.anime_id AS animeA, B.anime_id AS animeB, COUNT(*) AS co_occurence_cnt
        FROM user_anime A 
        LEFT JOIN user_anime B
        ON A.user_id = B.user_id 
        WHERE (ABS(A.user_anime_order - B.user_anime_order) BETWEEN 1 AND 10)
        GROUP BY A.anime_id, B.anime_id
        HAVING co_occurence_cnt >= 10
    ),
    anime_related_anime AS (
        SELECT animeA, animeB, related
        FROM `anime-rec-dev.processed_area.anime_anime` A
        INNER JOIN list_anime B
        ON A.animeA = B.anime_id
        INNER JOIN list_anime C
        ON A.animeB = C.anime_id
        WHERE A.related = 1
    ),
    anime_recommended_anime AS (
        SELECT animeA, animeB, num_recommenders
        FROM `anime-rec-dev.processed_area.anime_anime` A
        INNER JOIN list_anime B
        ON A.animeA = B.anime_id
        INNER JOIN list_anime C
        ON A.animeB = C.anime_id
        WHERE A.recommendation = 1 AND A.num_recommenders >= 5
    ),
    anime_anime AS (
        SELECT  A.animeA AS animeA, 
                A.animeB AS animeB, 
                COALESCE(A.co_occurence_cnt, 0) AS co_occurence_cnt, 
                COALESCE(B.related, 0) AS related, 
                COALESCE(C.num_recommenders, 0) AS num_recommenders
        FROM anime_co_watch_anime A
        FULL OUTER JOIN anime_related_anime B
        ON A.animeA = B.animeA AND A.animeB = B.animeB
        FULL OUTER JOIN anime_recommended_anime C
        ON A.animeA = C.animeA AND A.animeB = C.animeB
    ),
    ordered_anime_anime AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY animeA ORDER BY related DESC, num_recommenders DESC, co_occurence_cnt DESC) AS animeB_rank
        FROM anime_anime
    ),
    positive_pairs AS (
        SELECT A.animeA AS anchor_anime, A.animeB AS rel_anime_1, B.animeB AS rel_anime_2, 1 AS label
        FROM ordered_anime_anime A
        LEFT JOIN ordered_anime_anime B
        ON A.animeA = B.animeA AND A.animeB_rank + 10 < B.animeB_rank
        WHERE A.animeB_rank <= 100 AND B.animeB_rank <= 100
    ),
    negative_pairs AS (
        SELECT A.animeA AS anchor_anime, A.animeB AS rel_anime_1, B.animeB AS rel_anime_2, 0 AS label
        FROM ordered_anime_anime A
        LEFT JOIN ordered_anime_anime B
        ON A.animeA = B.animeA AND A.animeB_rank > B.animeB_rank + 10
        WHERE A.animeB_rank <= 100 AND B.animeB_rank <= 100
    ),
    all_pairs AS (
        SELECT anchor_anime, rel_anime_1, rel_anime_2, label FROM positive_pairs
        UNION DISTINCT
        SELECT anchor_anime, rel_anime_1, rel_anime_2, label FROM negative_pairs
    )
    """
    if mode == 'TRAIN':
        anime_anime_query += """
        SELECT *
        FROM all_pairs 
        WHERE ABS(MOD(FARM_FINGERPRINT(CONCAT(anchor_anime, rel_anime_1)), 10)) BETWEEN 0 AND 7
        """
    elif mode == 'VAL':
        anime_anime_query += """
        SELECT *
        FROM all_pairs 
        WHERE ABS(MOD(FARM_FINGERPRINT(CONCAT(anchor_anime, rel_anime_1)), 10)) = 8
        """
    else:
        anime_anime_query += """
        SELECT *
        FROM all_pairs 
        WHERE ABS(MOD(FARM_FINGERPRINT(CONCAT(anchor_anime, rel_anime_1)), 10)) = 9
        """
    return anime_anime_query

def sample_user_last_anime_watched():
    '''
        SQL query that returns for each user the last anime they enjoyed watching
    '''
    last_watched_query = """
        WITH 
        list_anime AS (
            SELECT anime_id, COUNT(*) AS cnt
            FROM `anime-rec-dev.processed_area.user_anime`
            WHERE status = 'completed'
            GROUP BY anime_id
            ORDER BY cnt DESC
            LIMIT 100
        ),
        user_anime_watch AS (
            SELECT A.user_id, A.anime_id, ROW_NUMBER() OVER (PARTITION BY A.user_id ORDER BY A.last_interaction_date DESC) AS user_anime_order
            FROM `anime-rec-dev.processed_area.user_anime` A
            INNER JOIN list_anime B
            ON A.anime_id = B.anime_id
            WHERE A.status = 'completed' 
            AND A.last_interaction_date IS NOT NULL
            AND (A.score IS NULL OR A.score > 5)
        )
        SELECT user_id, anime_id FROM user_anime_watch WHERE user_anime_order = 1
    """
    return last_watched_query
