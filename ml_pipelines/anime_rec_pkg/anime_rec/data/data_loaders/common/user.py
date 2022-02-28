from anime_rec.data.data_loaders.data_load_utils import CSVDataLoader

def load_list_user(data_path, file_format='csv', output_format='pandas'):
    if file_format == 'csv':
        if output_format in {'pd', 'pandas'}:
            return _load_list_user_csv_to_pd(data_path)
        elif output_format in {'np', 'numpy'}:
            return _load_list_user_csv_to_np(data_path)
        elif output_format in {'tfds'}:
            return _load_list_user_csv_to_tfds(data_path)
        else:
            raise(f"Output format {output_format} is not supported. Specify 'pd' or 'pandas'\
                for pandas dataframe, 'np' or 'numpy' for Numpy array and 'tfds' for tensorflow dataset")
    else:
        raise(f"File format {file_format} is not supported. Specify 'csv'")

def _load_list_user_csv_to_pd(data_path):
    '''
        Load list of all users from CSV to pandas DataFrame
    '''
    list_user = CSVDataLoader(data_path).load_pandas()
    list_user = list_user[['user_id']]
    return list_user

def _load_list_user_csv_to_np(data_path):
    '''
        Load list of all users from CSV to numpy array
    '''
    list_user = _load_list_user_csv_to_pd(data_path)
    list_user = list_user['user_id'].values
    return list_user

def _load_list_user_csv_to_tfds(data_path):
    '''
        Load list of all users from CSV to TF Dataset
    '''
    list_user = CSVDataLoader(data_path).load_tfds()
    list_user = list_user.map(lambda x :
        {
            'user_id' : x['user_id']
        }
    )
    return list_user