import numpy as np
import tensorflow as tf
from anime_rec.data.data_loaders.data_load_utils import CSVDataLoader

'''
    User Anime Retrieval Data Loader
'''
def load_user_anime_retrieval(data_path, file_format='csv', output_format='pandas'):

    if output_format in {'pd', 'pandas'}:
        return _load_user_anime_retrieval_to_pd(data_path, file_format)
    elif output_format in {'np', 'numpy'}:
        return _load_user_anime_retrieval_to_np(data_path, file_format)
    elif output_format in {'tfds'}:
        return _load_user_anime_retrieval_to_tfds(data_path, file_format)
    else:
        raise(f"Output format {output_format} is not supported. Specify 'pd' or 'pandas'\
            for pandas dataframe, 'np' or 'numpy' for Numpy array and 'tfds' for tensorflow dataset")

def _load_user_anime_retrieval_to_pd(data_path, file_format='csv'):

    if file_format=='csv':
        user_anime_retrieval = CSVDataLoader(data_path).load_pandas()
    else:
        raise(f"File format {file_format} is not supported. Specify 'csv'")
    
    user_anime_retrieval['user_id'] = user_anime_retrieval['user_id'].apply(str)
    user_anime_retrieval['anime_id'] = user_anime_retrieval['anime_id'].apply(str)
    user_anime_retrieval = user_anime_retrieval[['user_id', 'anime_id']]
    return user_anime_retrieval

def _load_user_anime_retrieval_to_np(data_path):

    user_anime_retrieval = _load_user_anime_retrieval_to_pd(data_path)
    user_anime_retrieval = user_anime_retrieval.values
    return user_anime_retrieval

def _load_user_anime_retrieval_to_tfds(data_path, file_format='csv'):

    if file_format=='csv':
        user_anime_retrieval = CSVDataLoader(data_path).load_tfds()
    else:
        raise(f"File format {file_format} is not supported. Specify 'csv'")
    
    user_anime_retrieval = user_anime_retrieval.map(lambda x : 
        {
            'user_id' : x['user_id'],
            'anime_id' : tf.strings.as_string(x['anime_id'])
        }
    )
    return user_anime_retrieval


'''
    User Anime Ranking Data Loader
'''
def load_user_anime_ranking(data_path, file_format='csv', output_format='pandas'):

    if output_format in {'pd', 'pandas'}:
        return _load_user_anime_ranking_to_pd(data_path, file_format)
    elif output_format in {'np', 'numpy'}:
        return _load_user_anime_ranking_to_np(data_path, file_format)
    elif output_format in {'tfds'}:
        return _load_user_anime_ranking_to_tfds(data_path, file_format)
    else:
        raise(f"Output format {output_format} is not supported. Specify 'pd' or 'pandas'\
            for pandas dataframe, 'np' or 'numpy' for Numpy array and 'tfds' for tensorflow dataset")

def _load_user_anime_ranking_to_pd(data_path, file_format='csv'):

    if file_format=='csv':
        user_anime_ranking = CSVDataLoader(data_path).load_pandas()
    else:
        raise(f"File format {file_format} is not supported. Specify 'csv'")
    
    user_anime_ranking['user_id'] = user_anime_ranking['user_id'].apply(str)
    user_anime_ranking['anime_id'] = user_anime_ranking['anime_id'].apply(str)
    user_anime_ranking['score'] = user_anime_ranking['score'].apply(float)
    user_anime_ranking = user_anime_ranking[['user_id', 'anime_id', 'score']]
    return user_anime_ranking

def _load_user_anime_ranking_to_np(data_path):

    user_anime_ranking = _load_user_anime_ranking_to_pd(data_path)
    user_anime_ranking = user_anime_ranking.values
    return user_anime_ranking

def _load_user_anime_ranking_to_tfds(data_path, file_format='csv'):

    if file_format=='csv':
        user_anime_ranking = CSVDataLoader(data_path).load_tfds()
    else:
        raise(f"File format {file_format} is not supported. Specify 'csv'")
    
    user_anime_ranking = user_anime_ranking.map(lambda x : 
        {
            'user_id' : x['user_id'],
            'anime_id' : tf.strings.as_string(x['anime_id']),
            'score' : x['score']
        }
    )
    return user_anime_ranking


'''
    User Anime List Ranking Data Loader
'''
def load_user_anime_list_ranking(data_path, file_format='csv', output_format='pandas'):
    if output_format in {'pd', 'pandas'}:
        return _load_user_anime_list_ranking_to_pd(data_path, file_format)
    elif output_format in {'np', 'numpy'}:
        return _load_user_anime_list_ranking_to_np(data_path, file_format)
    elif output_format in {'tfds'}:
        return _load_user_anime_list_ranking_to_tfds(data_path, file_format)
    else:
        raise(f"Output format {output_format} is not supported. Specify 'pd' or 'pandas'\
            for pandas dataframe, 'np' or 'numpy' for Numpy array and 'tfds' for tensorflow dataset")

def _load_user_anime_list_ranking_to_pd(data_path, file_format='csv'):

    if file_format=='csv':
        user_anime_list_ranking = CSVDataLoader(data_path).load_pandas()
    else:
        raise(f"File format {file_format} is not supported. Specify 'csv'")
    
    user_anime_list_ranking['user_id'] = user_anime_list_ranking['user_id'].apply(str)
    user_anime_list_ranking['anime_id'] = user_anime_list_ranking['anime_id'].apply(lambda x : np.array(str(x).split('|')))
    user_anime_list_ranking['score'] = user_anime_list_ranking['score'].apply(lambda x : np.array(map(float, str(x).split('|'))))
    user_anime_list_ranking = user_anime_list_ranking[['user_id', 'anime_id', 'score']]
    return user_anime_list_ranking

def _load_user_anime_list_ranking_to_np(data_path, file_format='csv'):

    user_anime_list_ranking = _load_user_anime_list_ranking_to_pd(data_path, file_format)
    user_anime_list_ranking = user_anime_list_ranking.values
    return user_anime_list_ranking

def _load_user_anime_list_ranking_to_tfds(data_path, file_format='csv'):

    if file_format=='csv':
        user_anime_list_ranking = CSVDataLoader(data_path).load_tfds()
    else:
        raise(f"File format {file_format} is not supported. Specify 'csv'")

    user_anime_list_ranking = user_anime_list_ranking.map(lambda x : {
        'user_id' : x['user_id'],
        'anime_id' : tf.strings.split(x['anime_id'], sep='|'),
        'score' : tf.strings.split(x['score'], sep='|')
    })
    user_anime_list_ranking = user_anime_list_ranking.map(lambda x : {
        'user_id' : tf.reshape(x['user_id'], shape = (1,)),
        'anime_id' : x['anime_id'],
        'score' : tf.strings.to_number(x['score'])
    })
    user_anime_list_ranking = user_anime_list_ranking.map(lambda x : {
        'user_id' : tf.repeat(x['user_id'], tf.shape(x['anime_id'])[0], axis = 0),
        'anime_id' : x['anime_id'],
        'score' : x['score']
    })
    return user_anime_list_ranking