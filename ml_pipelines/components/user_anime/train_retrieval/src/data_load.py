import os
import sys
import tempfile
import tensorflow as tf

sys.path.append(os.path.abspath(__file__ + "/../../../../../"))
sys.path.append(os.path.abspath(__file__ + "/../"))

from utils.gcs_utils import download_from_gcs_to_local

def load_user_anime_retrieval_dataset(data_path, batch_size=2048, shuffle=False):

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
    dataset = dataset.map(lambda x : 
        {
            'user_id' : x['user_id'],
            'anime_id' : tf.strings.as_string(x['anime_id'])
        }
    )
    if shuffle:
        dataset = dataset.shuffle(100_000, reshuffle_each_iteration=True)
    dataset = dataset.cache()
    return dataset
