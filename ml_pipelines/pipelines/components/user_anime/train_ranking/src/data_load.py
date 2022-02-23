import tempfile
import tensorflow as tf

from ml_package.utils.gcs_utils import download_from_gcs_to_local

def load_user_anime_rating_prediction_dataset(data_path, batch_size=2048, shuffle=False):
    if data_path.startswith('gs://') or data_path.startswith('/gcs/'):
        _, local_data_path = tempfile.mkstemp()
        download_from_gcs_to_local(data_path, local_data_path)
        data_path = local_data_path

    dataset = tf.data.experimental.make_csv_dataset(
        data_path,
        batch_size=batch_size,
        num_epochs=1,
        shuffle=False
    )
    dataset = dataset.map(lambda x : (
        {
            'user_id' : x['user_id'],
            'anime_id' : x['anime_id']
        },
        x['score']
    ))
    if shuffle:
        dataset = dataset.shuffle(100_000, reshuffle_each_iteration=True)
    dataset = dataset.cache()
    return dataset

def load_user_anime_list_ranking_dataset(data_path, batch_size=2048, shuffle=False):
    '''
        Load user anime list ranking dataset from csv file
    '''
    if data_path.startswith('gs://') or data_path.startswith('/gcs/'):
        _, local_data_path = tempfile.mkstemp()
        download_from_gcs_to_local(data_path, local_data_path)
        data_path = local_data_path

    dataset = tf.data.experimental.make_csv_dataset(
        data_path,
        batch_size=batch_size,
        num_epochs=1,
        shuffle=False
    )
    dataset = dataset.map(lambda x : {
        'user_id' : x['user_id'],
        'anime_id' : tf.strings.regex_replace(x['anime_id'], pattern = r"\[|\]|'", rewrite = ""),
        'score' : tf.strings.regex_replace(x['score'], pattern = r"\[|\]|'", rewrite = "")
    })
    dataset = dataset.map(lambda x : {
        'user_id' : x['user_id'],
        'anime_id' : tf.strings.split(x['anime_id']),
        'score' : tf.strings.split(x['score'])
    })
    dataset = dataset.map(lambda x : {
        'user_id' : x['user_id'],
        'anime_id' : x['anime_id'].to_tensor(),
        'score' : tf.strings.to_number(x['score']).to_tensor()
    })
    dataset = dataset.map(lambda x : {
        'user_id' : tf.repeat(tf.expand_dims(x['user_id'], 1), tf.shape(x['anime_id'])[1], axis = 1),
        'anime_id' : x['anime_id'],
        'score' : x['score']
    })
    dataset = dataset.map(lambda x : (
        {
            'user_id' : x['user_id'],
            'anime_id' : x['anime_id']
        },
        x['score']
    ))
    if shuffle:
        dataset = dataset.shuffle(100_000, reshuffle_each_iteration=True)
    dataset = dataset.cache()
    return dataset
