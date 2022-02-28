import os
import unittest

import tensorflow as tf

from anime_rec.data.data_loaders.common.anime import load_list_anime
from anime_rec.data.data_loaders.train.anime_anime import load_anime_anime_retrieval

from anime_rec.train.anime_anime import get_anime_anime_retrieval_model, train_anime_anime_retrieval_model, get_anime_anime_retrieval_index

class DataLoadTest(unittest.TestCase):

    def test_user_anime_retrieval_dataload(self):

        dir_path = os.path.dirname(os.path.realpath(__file__))
        data_path = os.path.join(dir_path, 'sample_data/anime_anime/retrieval/train')

        dataset = load_anime_anime_retrieval(data_path, 'csv', 'tfds').batch(16)

        assert('animeA' in dataset.element_spec)
        assert('animeB' in dataset.element_spec)
        assert(len(dataset.element_spec.keys()) == 2)

        assert(dataset.element_spec['animeA'].dtype == tf.string)
        assert(dataset.element_spec['animeB'].dtype == tf.string)

        for x in dataset.take(1):
            assert(x['animeA'].shape == (16,))
            assert(x['animeB'].shape == (16,))

class TrainingTest(unittest.TestCase):

    def test_weights_updated(self):

        dir_path = os.path.dirname(os.path.realpath(__file__))

        all_anime_data_path = os.path.join(dir_path, 'sample_data/list_anime')
        train_data_path = os.path.join(dir_path, 'sample_data/anime_anime/retrieval/train')

        list_anime = load_list_anime(all_anime_data_path, 'csv', 'numpy')
        train_dataset = load_anime_anime_retrieval(train_data_path, 'csv', 'tfds').batch(2048)

        model = get_anime_anime_retrieval_model(
            list_anime,
            128,
            0.005,
            'adam'
        )

        anime_embeddings_1 = model.anime_model.anime_embedding_layer.get_weights()

        model.fit(train_dataset, epochs = 1, verbose = 0)

        anime_embeddings_2 = model.anime_model.anime_embedding_layer.get_weights()

        tf.test.TestCase().assertNotAllEqual(a=anime_embeddings_1, b=anime_embeddings_2)

    def test_one_batch_overfit(self):

        dir_path = os.path.dirname(os.path.realpath(__file__))

        all_anime_data_path = os.path.join(dir_path, 'sample_data/list_anime')
        train_data_path = os.path.join(dir_path, 'sample_data/anime_anime/retrieval/train')
        validation_data_path = os.path.join(dir_path, 'sample_data/anime_anime/retrieval/val')

        list_anime = load_list_anime(all_anime_data_path, 'csv', 'numpy')
        train_dataset = load_anime_anime_retrieval(train_data_path, 'csv', 'tfds').batch(16).take(1)
        val_dataset = load_anime_anime_retrieval(validation_data_path, 'csv', 'tfds').batch(16).take(1)
        
        hyperparameters = {
            'anime_embedding_size' : 128,
            'learning_rate' : 0.005,
            'optimizer' : 'adam',
            'early_stop_num_epochs' : 100,
            'max_num_epochs' : 100
        }

        model = train_anime_anime_retrieval_model(
            list_anime,
            train_dataset,
            val_dataset,
            hyperparameters
        )

        train_metrics = model.evaluate(train_dataset, return_dict=True)
        val_metrics = model.evaluate(val_dataset, return_dict=True)

        assert(val_metrics['loss'] > 5.0 * train_metrics['loss'])
        assert(val_metrics['factorized_top_k/top_10_categorical_accuracy'] < 0.5 * train_metrics['factorized_top_k/top_10_categorical_accuracy'])
    
    def test_index(self):
        
        dir_path = os.path.dirname(os.path.realpath(__file__))

        all_anime_data_path = os.path.join(dir_path, 'sample_data/list_anime')
        train_data_path = os.path.join(dir_path, 'sample_data/anime_anime/retrieval/train')
        validation_data_path = os.path.join(dir_path, 'sample_data/anime_anime/retrieval/val')

        list_anime = load_list_anime(all_anime_data_path, 'csv', 'numpy')
        train_dataset = load_anime_anime_retrieval(train_data_path, 'csv', 'tfds').batch(2048).shuffle(100_000, reshuffle_each_iteration=True)
        val_dataset = load_anime_anime_retrieval(validation_data_path, 'csv', 'tfds').batch(2048)
        
        hyperparameters = {
            'anime_embedding_size' : 128,
            'learning_rate' : 0.005,
            'optimizer' : 'adam',
            'early_stop_num_epochs' : 5,
            'max_num_epochs' : 25
        }

        model = train_anime_anime_retrieval_model(
            list_anime,
            train_dataset,
            val_dataset,
            hyperparameters
        )

        index_model = get_anime_anime_retrieval_index(list_anime, model, k = 2)
        retrieved_results = index_model(tf.constant([list_anime[0]]))[1][0]

        assert(retrieved_results.shape == (2,))