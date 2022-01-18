import os
import sys
import tempfile
import tensorflow as tf

sys.path.append(os.path.abspath(__file__ + "/../../../../../"))
sys.path.append(os.path.abspath(__file__ + "/../"))

from utils.gcs_utils import download_from_gcs_to_local

def load_anime_anime_pair_classification_dataset(
        data_path,
        batch_size=2048,
        shuffle=False
):
    if data_path.startswith('gs://') or data_path.startswith('/gcs/'):
        _, local_data_path = tempfile.mkstemp()
        download_from_gcs_to_local(data_path, local_data_path)
        data_path = local_data_path
    
    dataset = tf.data.experimental.make_csv_dataset(
        data_path,
        batch_size=batch_size,
        label_name='label',
        num_epochs=1,
        shuffle=False
    )
    dataset = dataset.map(lambda x, y: 
        (
            {
                'anchor_anime' : tf.strings.as_string(x['anchor_anime']),
                'rel_anime_1' : tf.strings.as_string(x['rel_anime_1']),
                'rel_anime_2' : tf.strings.as_string(x['rel_anime_2']),
            },
            y
        )
    )
    if shuffle:
        dataset = dataset.shuffle(100_000, reshuffle_each_iteration=True)
    dataset = dataset.cache()
    return dataset
