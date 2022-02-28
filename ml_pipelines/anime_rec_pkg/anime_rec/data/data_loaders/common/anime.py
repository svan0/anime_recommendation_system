import tensorflow as tf
from anime_rec.data.data_loaders.data_load_utils import CSVDataLoader

def load_list_anime(data_path, file_format='csv', output_format='pandas'):
    if file_format == 'csv':
        if output_format in {'pd', 'pandas'}:
            return _load_list_anime_csv_to_pd(data_path)
        elif output_format in {'np', 'numpy'}:
            return _load_list_anime_csv_to_np(data_path)
        elif output_format in {'tfds'}:
            return _load_list_anime_csv_to_tfds(data_path)
        else:
            raise(f"Output format {output_format} is not supported. Specify 'pd' or 'pandas'\
                for pandas dataframe, 'np' or 'numpy' for Numpy array and 'tfds' for tensorflow dataset")
    else:
        raise(f"File format {file_format} is not supported. Specify 'csv'")

def _load_list_anime_csv_to_pd(data_path):
    '''
        Load list of all anime from CSV to pandas DataFrame
    '''
    list_anime = CSVDataLoader(data_path).load_pandas()
    list_anime['anime_id'] = list_anime['anime_id'].apply(str)
    list_anime = list_anime[['anime_id']]
    return list_anime

def _load_list_anime_csv_to_np(data_path):
    '''
        Load list of all anime from CSV to numpy array
    '''
    list_anime = _load_list_anime_csv_to_pd(data_path)
    list_anime = list_anime['anime_id'].values
    return list_anime

def _load_list_anime_csv_to_tfds(data_path):
    '''
        Load list of all anime from CSV to TF Dataset
    '''
    list_anime = CSVDataLoader(data_path).load_tfds()
    list_anime = list_anime.map(lambda x :
        {
            'anime_id' : tf.strings.as_string(x['anime_id'])
        }
    )
    return list_anime