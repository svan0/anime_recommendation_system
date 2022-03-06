import numpy as np
import tensorflow as tf
from anime_rec.data.data_loaders.data_load_utils import CSVDataLoader

'''
    Anime Anime Retrieval Training Data Loaders
'''
def load_anime_anime_retrieval(data_path, file_format='csv', output_format='pandas'):

    if output_format in {'pd', 'pandas'}:
        return _load_anime_anime_retrieval_to_pd(data_path, file_format)
    elif output_format in {'np', 'numpy'}:
        return _load_anime_anime_retrieval_to_np(data_path, file_format)
    elif output_format in {'tfds'}:
        return _load_anime_anime_retrieval_to_tfds(data_path, file_format)
    else:
        raise(f"Output format {output_format} is not supported. Specify 'pd' or 'pandas'\
            for pandas dataframe, 'np' or 'numpy' for Numpy array and 'tfds' for tensorflow dataset")

def _load_anime_anime_retrieval_to_pd(data_path, file_format='csv'):

    if file_format=='csv':
        anime_anime_retrieval = CSVDataLoader(data_path).load_pandas()
    else:
        raise(f"File format {file_format} is not supported. Specify 'csv'")
    
    anime_anime_retrieval['anime_id'] = anime_anime_retrieval['anime_id'].apply(str)
    anime_anime_retrieval['retrieved_anime_id'] = anime_anime_retrieval['retrieved_anime_id'].apply(str)
    anime_anime_retrieval = anime_anime_retrieval[['anime_id', 'retrieved_anime_id']]
    return anime_anime_retrieval

def _load_anime_anime_retrieval_to_np(data_path):

    user_anime_retrieval = _load_anime_anime_retrieval_to_pd(data_path)
    user_anime_retrieval = user_anime_retrieval.values
    return user_anime_retrieval

def _load_anime_anime_retrieval_to_tfds(data_path, file_format='csv'):

    if file_format=='csv':
        anime_anime_retrieval = CSVDataLoader(data_path).load_tfds()
    else:
        raise(f"File format {file_format} is not supported. Specify 'csv'")
    
    anime_anime_retrieval = anime_anime_retrieval.map(lambda x : 
        {
            'anime_id' : tf.strings.as_string(x['anime_id']),
            'retrieved_anime_id' : tf.strings.as_string(x['retrieved_anime_id'])
        }
    )
    return anime_anime_retrieval


'''
    Anime Anime Ranking Training Data Loaders
'''
def load_anime_anime_pair_classification_ranking(data_path, file_format='csv', output_format='pandas'):

    if output_format in {'pd', 'pandas'}:
        return _load_anime_anime_pair_classification_ranking_to_pd(data_path, file_format)
    elif output_format in {'np', 'numpy'}:
        return _load_anime_anime_pair_classification_ranking_to_np(data_path, file_format)
    elif output_format in {'tfds'}:
        return _load_anime_anime_pair_classification_ranking_to_tfds(data_path, file_format)
    else:
        raise(f"Output format {output_format} is not supported. Specify 'pd' or 'pandas'\
            for pandas dataframe, 'np' or 'numpy' for Numpy array and 'tfds' for tensorflow dataset")

def _load_anime_anime_pair_classification_ranking_to_pd(data_path, file_format='csv'):

    if file_format=='csv':
        anime_anime_ranking = CSVDataLoader(data_path).load_pandas()
    else:
        raise(f"File format {file_format} is not supported. Specify 'csv'")
    
    anime_anime_ranking['anime_id'] = anime_anime_ranking['anime_id'].apply(str)
    anime_anime_ranking['retrieved_anime_id_1'] = anime_anime_ranking['retrieved_anime_id_1'].apply(str)
    anime_anime_ranking['retrieved_anime_id_2'] = anime_anime_ranking['retrieved_anime_id_2'].apply(str)
    anime_anime_ranking['label'] = anime_anime_ranking['label'].apply(float)
    anime_anime_ranking = anime_anime_ranking[['anime_id', 'retrieved_anime_id_1', 'retrieved_anime_id_2s', 'label']]
    return anime_anime_ranking

def _load_anime_anime_pair_classification_ranking_to_np(data_path):

    anime_anime_ranking = _load_anime_anime_pair_classification_ranking_to_pd(data_path)
    anime_anime_ranking = anime_anime_ranking.values
    return anime_anime_ranking

def _load_anime_anime_pair_classification_ranking_to_tfds(data_path, file_format='csv'):

    if file_format=='csv':
        anime_anime_ranking = CSVDataLoader(data_path).load_tfds()
    else:
        raise(f"File format {file_format} is not supported. Specify 'csv'")
    
    anime_anime_ranking = anime_anime_ranking.map(lambda x : 
        {
            'anime_id' : tf.strings.as_string(x['anime_id']),
            'retrieved_anime_id_1' : tf.strings.as_string(x['retrieved_anime_id_1']),
            'retrieved_anime_id_2' : tf.strings.as_string(x['retrieved_anime_id_2']),
            'label' : x['label']
        }
    )
    return anime_anime_ranking

