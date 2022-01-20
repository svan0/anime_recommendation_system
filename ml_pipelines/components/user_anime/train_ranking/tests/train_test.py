import os
import sys
import unittest

import tensorflow as tf

sys.path.append(os.path.abspath(__file__ + "/../../../../../"))
sys.path.append(os.path.abspath(__file__ + "/../../"))

from utils.data_load_utils import load_list_anime_np, load_list_user_np
from src.data_load import load_user_anime_list_ranking_dataset
from src.train import train_user_anime_list_ranking_model

class TrainingTest(unittest.TestCase):

    def test_one_batch_overfit(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))

        all_anime_data_path = os.path.join(dir_path, 'sample_data/list_anime.csv')
        all_user_data_path = os.path.join(dir_path, 'sample_data/list_user.csv')
        train_data_path = os.path.join(dir_path, 'sample_data/train.csv')
        validation_data_path = os.path.join(dir_path, 'sample_data/val.csv')

        list_anime = load_list_anime_np(all_anime_data_path)
        list_user = load_list_user_np(all_user_data_path)
        train_dataset = load_user_anime_list_ranking_dataset(train_data_path, batch_size=16,shuffle=False).take(1)
        val_dataset = load_user_anime_list_ranking_dataset(validation_data_path, batch_size=16,shuffle=False).take(1)

        hyperparameters = {
            'anime_embedding_size' : 128,
            'user_embedding_size' : 128,
            'scoring_layer_size' : 128,
            'learning_rate' : 0.005,
            'optimizer' : 'adam',
            'early_stop_num_epochs' : 100,
            'max_num_epochs' : 100
        }

        model = train_user_anime_list_ranking_model(
            list_anime,
            list_user,
            train_dataset,
            val_dataset,
            hyperparameters
        )

        train_metrics = model.evaluate(train_dataset, return_dict=True)
        val_metrics = model.evaluate(val_dataset, return_dict=True)


        assert(val_metrics['loss'] > 3.0 * train_metrics['loss'])
        assert(val_metrics['root_mean_squared_error'] > 5.0 * train_metrics['root_mean_squared_error'])