import os
import unittest

from src.task import run_task

class TaskTest(unittest.TestCase):
    def test_task_runs(self):

        dir_path = os.path.dirname(os.path.realpath(__file__))

        all_anime_data_path = os.path.join(dir_path, 'sample_data/list_anime.csv')
        all_user_data_path = os.path.join(dir_path, 'sample_data/list_user.csv')
        train_data_path = os.path.join(dir_path, 'sample_data/train.csv')
        validation_data_path = os.path.join(dir_path, 'sample_data/val.csv')
        test_data_path = os.path.join(dir_path, 'sample_data/test.csv')
        model_path = os.path.join(dir_path, 'sample_data/trained_model')
        metrics_path = os.path.join(dir_path, 'sample_data/metrics.json')

        hyperparameters = {
            'anime_embedding_size' : 128,
            'user_embedding_size' : 128,
            'scoring_layer_size' : 128,
            'learning_rate' : 0.005,
            'optimizer' : 'adam',
            'early_stop_num_epochs' : 5,
            'max_num_epochs' : 25
        }

        run_task(
            'list_ranking',
            train_data_path,
            validation_data_path,
            test_data_path,
            all_anime_data_path,
            all_user_data_path,
            model_path,
            metrics_path,
            hyperparameters
        )