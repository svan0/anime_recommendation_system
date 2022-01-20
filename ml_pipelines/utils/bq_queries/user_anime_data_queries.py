'''
    SQL queries used in the pipeline that recommends animes
    based on the what the user watched (collaborative filtering)
'''
def all_anime_query():
    '''
        SQL query that returns list of all unique animes
    '''
    query = '''
        SELECT DISTINCT(anime_id) FROM `anime-rec-dev.processed_area.user_anime`
    '''
    return query

def all_user_query():
    '''
        SQL query that returns all unique users that have at least
        50 completed / scored animes
    '''
    query = f"""
    WITH list_users AS (
        SELECT user_id, COUNT(*) AS cnt
        FROM `anime-rec-dev.processed_area.user_anime`
        WHERE status = 'completed' AND score IS NOT NULL AND score > 0 AND last_interaction_date IS NOT NULL
        GROUP BY user_id
        HAVING cnt >= 50
    )
    SELECT user_id FROM list_users
    """
    return query

def user_anime_retrieval_query(mode='TRAIN'):
    '''
        SQL query that returns train, validation or test data
        for the collaborative filtering retrieval model
        Returns pairs of user / anime where the user completed the anime
        Test data are the last 10 completed animes per user
        Val data are the last 10 completed animes previous to the test data
        Train data are the other completed animes per user
    '''
    query = """
    WITH 
    list_users AS (
        SELECT user_id, COUNT(*) AS cnt
        FROM `anime-rec-dev.processed_area.user_anime`
        WHERE status = 'completed' 
              AND score IS NOT NULL 
              AND score > 0 
              AND last_interaction_date IS NOT NULL
        GROUP BY user_id
        HAVING cnt >= 50
    ),
    user_anime_data AS (
        SELECT A.user_id, A.anime_id, A.status, A.score, A.last_interaction_date
        FROM `anime-rec-dev.processed_area.user_anime` A
        INNER JOIN list_users B
        ON A.user_id = B.user_id
        WHERE A.status = 'completed'
    ),
    user_anime_data_with_completed_order AS (
        SELECT user_id, anime_id, status, score, last_interaction_date, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY last_interaction_date IS NULL ASC, last_interaction_date DESC) AS completed_order
        FROM user_anime_data
    )
    """
    if mode == 'TRAIN':
        query += """
        SELECT user_id, anime_id 
        FROM user_anime_data_with_completed_order 
        WHERE completed_order >= 21
        """
    elif mode == 'VAL':
        query += """
        SELECT user_id, anime_id 
        FROM user_anime_data_with_completed_order 
        WHERE completed_order BETWEEN 11 AND 20
        """
    else:
        query += """
        SELECT user_id, anime_id 
        FROM user_anime_data_with_completed_order 
        WHERE completed_order BETWEEN 1 AND 10
        """
    return query

def user_anime_list_ranking_query(mode='TRAIN'):
    '''
        SQL query that returns train, validation or test data
        for the collaborative filtering list ranking model
        For each user we return a list of 10 scored animes
        and a list of the score
        The model will need to learn to rank those lists of animes
    '''
    query = """
    WITH 
    list_users AS (
        SELECT user_id, COUNT(*) AS cnt
        FROM `anime-rec-dev.processed_area.user_anime`
        WHERE status = 'completed' 
              AND score IS NOT NULL 
              AND score > 0 
              AND last_interaction_date IS NOT NULL
        GROUP BY user_id
        HAVING cnt >= 50
    ),
    user_anime_data AS (
        SELECT A.user_id, A.anime_id, A.status, A.score, A.last_interaction_date, ROW_NUMBER() OVER (PARTITION BY A.user_id ORDER BY A.last_interaction_date IS NULL ASC, A.last_interaction_date DESC) AS completed_order
        FROM `anime-rec-dev.processed_area.user_anime` A
        INNER JOIN list_users B
        ON A.user_id = B.user_id
        WHERE A.status = 'completed' AND A.score IS NOT NULL AND A.score > 0
    ),
    train_data AS (
        SELECT user_id, anime_id, score
        FROM user_anime_data
        WHERE completed_order >= 21
    ),
    train_data_random_order AS (
        SELECT user_id, 
               anime_id, 
               score, 
               ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY RAND()) - 1 AS random_order_anime_per_user
        FROM train_data
    ),
    train_data_list AS (
        SELECT user_id, ARRAY_AGG(anime_id) AS anime_id, ARRAY_AGG(score) AS score 
        FROM train_data_random_order
        GROUP BY user_id, DIV(random_order_anime_per_user, 10)
        HAVING ARRAY_LENGTH(anime_id) = 10
    ),
    val_data AS (
        SELECT user_id, anime_id, score
        FROM user_anime_data
        WHERE completed_order BETWEEN 11 AND 20
    ),
    val_data_list AS (
        SELECT user_id, ARRAY_AGG(anime_id) AS anime_id, ARRAY_AGG(score) AS score 
        FROM val_data 
        GROUP BY user_id
    ),
    test_data AS (
        SELECT user_id, anime_id, score
        FROM user_anime_data
        WHERE completed_order BETWEEN 1 AND 10
    ),
    test_data_list AS (
        SELECT user_id, ARRAY_AGG(anime_id) AS anime_id, ARRAY_AGG(score) AS score 
        FROM test_data 
        GROUP BY user_id
    )
    """
    if mode == 'TRAIN':
        query += """
        SELECT *
        FROM train_data_list
        """
    elif mode == 'VAL':
        query += """
        SELECT *
        FROM val_data_list
        """
    else:
        query += """
        SELECT *
        FROM test_data_list
        """
    return query

def user_retrieved_animes(retrieved_data_name):
    '''
        SQL queries that takes all the retrieved animes for each user
        and only returns retrieved animes that haven't been seen
        by the user
    '''
    retrieved_anime_query = f"""
        SELECT A.user_id, A.retrieved_anime_id
        FROM {retrieved_data_name} A
        LEFT JOIN `anime-rec-dev.processed_area.user_anime` B
        ON A.user_id = B.user_id AND A.retrieved_anime_id = B.anime_id
        WHERE B.status IS NULL
    """
    return retrieved_anime_query


'''
    Similar queries but on a sample of 100 animes and 100 users
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

def sample_all_user_query():
    query = """
    WITH list_users AS (
        SELECT user_id, COUNT(*) AS cnt
        FROM `anime-rec-dev.processed_area.user_anime`
        WHERE status = 'completed' 
              AND score IS NOT NULL 
              AND score > 0 
              AND last_interaction_date IS NOT NULL
        GROUP BY user_id
        ORDER BY cnt DESC, user_id
        LIMIT 100
    )
    SELECT user_id FROM list_users
    """
    return query

def sample_user_anime_retrieval_query(mode='TRAIN'):
    query = """
    WITH 
    list_users AS (
        SELECT user_id, COUNT(*) AS cnt
        FROM `anime-rec-dev.processed_area.user_anime`
        WHERE status = 'completed' 
              AND score IS NOT NULL 
              AND score > 0 
              AND last_interaction_date IS NOT NULL
        GROUP BY user_id
        ORDER BY cnt DESC, user_id
        LIMIT 100
    ),
    list_anime AS (
        SELECT anime_id, COUNT(*) AS cnt
        FROM `anime-rec-dev.processed_area.user_anime`
        WHERE status = 'completed'
        GROUP BY anime_id
        ORDER BY cnt DESC, anime_id
        LIMIT 100
    ),
    user_anime_data AS (
        SELECT A.user_id, A.anime_id, A.status, A.score, A.last_interaction_date
        FROM `anime-rec-dev.processed_area.user_anime` A
        INNER JOIN list_users B
        ON A.user_id = B.user_id
        INNER JOIN list_anime C
        ON A.anime_id = C.anime_id
        WHERE A.status = 'completed'
    ),
    user_anime_data_with_completed_order AS (
        SELECT user_id, anime_id, status, score, last_interaction_date, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY last_interaction_date IS NULL ASC, last_interaction_date DESC) AS completed_order
        FROM user_anime_data
    )
    """
    if mode == 'TRAIN':
        query += """
        SELECT user_id, anime_id 
        FROM user_anime_data_with_completed_order 
        WHERE completed_order >= 21
        """
    elif mode == 'VAL':
        query += """
        SELECT user_id, anime_id 
        FROM user_anime_data_with_completed_order 
        WHERE completed_order BETWEEN 11 AND 20
        """
    else:
        query += """
        SELECT user_id, anime_id 
        FROM user_anime_data_with_completed_order 
        WHERE completed_order BETWEEN 1 AND 10
        """
    return query

def sample_user_anime_list_ranking_query(mode='TRAIN'):
    query = """
    WITH 
    list_users AS (
        SELECT user_id, COUNT(*) AS cnt
        FROM `anime-rec-dev.processed_area.user_anime`
        WHERE status = 'completed' 
              AND score IS NOT NULL 
              AND score > 0 
              AND last_interaction_date IS NOT NULL
        GROUP BY user_id
        ORDER BY cnt DESC, user_id
        LIMIT 100
    ),
    list_anime AS (
        SELECT anime_id, COUNT(*) AS cnt
        FROM `anime-rec-dev.processed_area.user_anime`
        WHERE status = 'completed'
        GROUP BY anime_id
        ORDER BY cnt DESC, anime_id
        LIMIT 100
    ),
    user_anime_data AS (
        SELECT A.user_id, A.anime_id, A.status, A.score, A.last_interaction_date, ROW_NUMBER() OVER (PARTITION BY A.user_id ORDER BY A.last_interaction_date IS NULL ASC, A.last_interaction_date DESC) AS completed_order
        FROM `anime-rec-dev.processed_area.user_anime` A
        INNER JOIN list_users B
        ON A.user_id = B.user_id
        INNER JOIN list_anime C
        ON A.anime_id = C.anime_id
        WHERE A.status = 'completed' AND A.score IS NOT NULL AND A.score > 0
    ),
    train_data AS (
        SELECT user_id, anime_id, score
        FROM user_anime_data
        WHERE completed_order >= 21
    ),
    train_data_random_order AS (
        SELECT user_id, 
               anime_id, 
               score, 
               ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY RAND()) - 1 AS random_order_anime_per_user
        FROM train_data
    ),
    train_data_list AS (
        SELECT user_id, ARRAY_AGG(anime_id) AS anime_id, ARRAY_AGG(score) AS score 
        FROM train_data_random_order
        GROUP BY user_id, DIV(random_order_anime_per_user, 10)
        HAVING ARRAY_LENGTH(anime_id) = 10
    ),
    val_data AS (
        SELECT user_id, anime_id, score
        FROM user_anime_data
        WHERE completed_order BETWEEN 11 AND 20
    ),
    val_data_list AS (
        SELECT user_id, ARRAY_AGG(anime_id) AS anime_id, ARRAY_AGG(score) AS score 
        FROM val_data 
        GROUP BY user_id
    ),
    test_data AS (
        SELECT user_id, anime_id, score
        FROM user_anime_data
        WHERE completed_order BETWEEN 1 AND 10
    ),
    test_data_list AS (
        SELECT user_id, ARRAY_AGG(anime_id) AS anime_id, ARRAY_AGG(score) AS score 
        FROM test_data 
        GROUP BY user_id
    )
    """
    if mode == 'TRAIN':
        query += """
        SELECT *
        FROM train_data_list
        """
    elif mode == 'VAL':
        query += """
        SELECT *
        FROM val_data_list
        """
    else:
        query += """
        SELECT *
        FROM test_data_list
        """
    return query

