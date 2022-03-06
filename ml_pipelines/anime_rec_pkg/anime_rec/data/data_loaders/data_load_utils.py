import pandas as pd
import tempfile
import glob
import os
import tensorflow as tf

from anime_rec.data.data_loaders.gcs_utils import download_from_gcs_to_local

class CSVDataLoader:
    def __init__(self, path):
        self.path = path
        if self.path.startswith('gs://') or self.path.startswith('/gcs/'):
            self._download_data_from_gcs()
        if os.path.isdir(self.path):
            self.path = self.path + "/*"

    def _download_data_from_gcs(self):
        local_data_folder = tempfile.mkdtemp()
        download_from_gcs_to_local(self.path, local_data_folder)
        self.path = local_data_folder

    def load_pandas(self):
        list_files = glob.glob(self.path)
        list_dataframes = map(lambda x : pd.read_csv(x, keep_default_na=False), list_files)
        dataframe = pd.concat(list_dataframes)
        return dataframe
    
    def load_numpy(self):
        return self.load_pandas().values
    
    def load_tfds(self):
        dataset = tf.data.experimental.make_csv_dataset(
            self.path,
            batch_size=1,
            num_epochs=1,
            shuffle=False
        )
        dataset = dataset.unbatch()
        return dataset


