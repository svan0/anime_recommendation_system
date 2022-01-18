import os
import sys
import unittest

import tensorflow as tf

sys.path.append(os.path.abspath(__file__ + "/../../../../../"))
sys.path.append(os.path.abspath(__file__ + "/../../"))

from utils.data_load_utils import load_list_anime_np, load_list_user_np
from src.data_load import load_user_anime_retrieval_dataset
from src.train import get_user_anime_retrieval_model
from src.train import train_user_anime_retrieval_model

class TrainingTest(unittest.TestCase):

    def test_weights_updated(self):

        dir_path = os.path.dirname(os.path.realpath(__file__))

        all_anime_data_path = os.path.join(dir_path, 'sample_data/list_anime.csv')
        all_user_data_path = os.path.join(dir_path, 'sample_data/list_user.csv')
        train_data_path = os.path.join(dir_path, 'sample_data/train.csv')

        list_anime = load_list_anime_np(all_anime_data_path)
        list_user = load_list_user_np(all_user_data_path)
        train_dataset = load_user_anime_retrieval_dataset(train_data_path, batch_size=2048,shuffle=False)

        model = get_user_anime_retrieval_model(
            list_anime,
            list_user,
            128,
            0.005,
            'adam'
        )
        user_embeddings_1 = model.user_model.user_embedding_layer.get_weights()
        anime_embeddings_1 = model.anime_model.anime_embedding_layer.get_weights()

        model.fit(train_dataset, epochs = 1, verbose = 0)

        user_embeddings_2 = model.user_model.user_embedding_layer.get_weights()
        anime_embeddings_2 = model.anime_model.anime_embedding_layer.get_weights()

        tf.test.TestCase().assertNotAllEqual(a=user_embeddings_1, b=user_embeddings_2)
        tf.test.TestCase().assertNotAllEqual(a=anime_embeddings_1, b=anime_embeddings_2)

    def test_one_batch_overfit(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))

        all_anime_data_path = os.path.join(dir_path, 'sample_data/list_anime.csv')
        all_user_data_path = os.path.join(dir_path, 'sample_data/list_user.csv')
        train_data_path = os.path.join(dir_path, 'sample_data/train.csv')
        validation_data_path = os.path.join(dir_path, 'sample_data/val.csv')

        list_anime = load_list_anime_np(all_anime_data_path)
        list_user = load_list_user_np(all_user_data_path)
        train_dataset = load_user_anime_retrieval_dataset(train_data_path, batch_size=16,shuffle=False).take(1)
        val_dataset = load_user_anime_retrieval_dataset(validation_data_path, batch_size=16,shuffle=False).take(1)
        
        hyperparameters = {
            'user_anime_embedding_size' : 128,
            'learning_rate' : 0.005,
            'optimizer' : 'adam',
            'early_stop_num_epochs' : 100,
            'max_num_epochs' : 100
        }

        model = train_user_anime_retrieval_model(
            list_anime,
            list_user,
            train_dataset,
            val_dataset,
            hyperparameters
        )

        train_metrics = model.evaluate(train_dataset, return_dict=True)
        val_metrics = model.evaluate(val_dataset, return_dict=True)

        assert(val_metrics['loss'] > 10.0 * train_metrics['loss'])
        assert(val_metrics['factorized_top_k/top_10_categorical_accuracy'] < 0.5 * train_metrics['factorized_top_k/top_10_categorical_accuracy'])
