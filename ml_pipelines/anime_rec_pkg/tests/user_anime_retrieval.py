import os
import unittest
import pandas as pd

import tensorflow as tf

from anime_rec.data.data_loaders.common.anime import load_list_anime
from anime_rec.data.data_loaders.common.user import load_list_user
from anime_rec.data.data_loaders.train.user_anime import load_user_anime_retrieval as train_load_user_anime_retrieval
from anime_rec.train.user_anime import get_user_anime_retrieval_model, train_user_anime_retrieval_model, get_user_anime_retrieval_index
from anime_rec.data.data_loaders.infer.user_anime import load_user_anime_retrieval as infer_load_user_anime_retrieval
from anime_rec.infer.user_anime import infer_user_anime_retrieval

class Test1DataLoadTest(unittest.TestCase):

    def test_user_anime_retrieval_dataload(self):

        dir_path = os.path.dirname(os.path.realpath(__file__))
        data_path = os.path.join(dir_path, 'sample_data/user_anime/retrieval/train')

        dataset = train_load_user_anime_retrieval(data_path, 'csv', 'tfds').batch(16)

        assert('user_id' in dataset.element_spec)
        assert('anime_id' in dataset.element_spec)
        assert(len(dataset.element_spec.keys()) == 2)

        assert(dataset.element_spec['user_id'].dtype == tf.string)
        assert(dataset.element_spec['anime_id'].dtype == tf.string)

        for x in dataset.take(1):
            assert(x['user_id'].shape == (16,))
            assert(x['anime_id'].shape == (16,))

class Test2TrainingTest(unittest.TestCase):

    def test_weights_updated(self):

        dir_path = os.path.dirname(os.path.realpath(__file__))

        all_anime_data_path = os.path.join(dir_path, 'sample_data/list_anime')
        all_user_data_path = os.path.join(dir_path, 'sample_data/list_user')
        train_data_path = os.path.join(dir_path, 'sample_data/user_anime/retrieval/train')

        list_anime = load_list_anime(all_anime_data_path, 'csv', 'numpy')
        list_user = load_list_user(all_user_data_path, 'csv', 'numpy')
        train_dataset = train_load_user_anime_retrieval(train_data_path, 'csv', 'tfds').batch(2048)

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

        all_anime_data_path = os.path.join(dir_path, 'sample_data/list_anime')
        all_user_data_path = os.path.join(dir_path, 'sample_data/list_user')
        train_data_path = os.path.join(dir_path, 'sample_data/user_anime/retrieval/train')
        validation_data_path = os.path.join(dir_path, 'sample_data/user_anime/retrieval/val')

        list_anime = load_list_anime(all_anime_data_path, 'csv', 'numpy')
        list_user = load_list_user(all_user_data_path, 'csv', 'numpy')
        train_dataset = train_load_user_anime_retrieval(train_data_path, 'csv', 'tfds').batch(16).take(1)
        val_dataset = train_load_user_anime_retrieval(validation_data_path, 'csv', 'tfds').batch(16).take(1)
        
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

    def test_index(self):
        
        dir_path = os.path.dirname(os.path.realpath(__file__))

        all_anime_data_path = os.path.join(dir_path, 'sample_data/list_anime')
        all_user_data_path = os.path.join(dir_path, 'sample_data/list_user')
        train_data_path = os.path.join(dir_path, 'sample_data/user_anime/retrieval/train')
        validation_data_path = os.path.join(dir_path, 'sample_data/user_anime/retrieval/val')

        list_anime = load_list_anime(all_anime_data_path, 'csv', 'numpy')
        list_user = load_list_user(all_user_data_path, 'csv', 'numpy')
        train_dataset = train_load_user_anime_retrieval(train_data_path, 'csv', 'tfds').batch(16).take(1)
        val_dataset = train_load_user_anime_retrieval(validation_data_path, 'csv', 'tfds').batch(16).take(1)
        
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

        index_model = get_user_anime_retrieval_index(list_anime, model, k = 2)
        retrieved_results = index_model(tf.constant([list_user[0]]))[1][0]

        tf.saved_model.save(
            index_model,
            os.path.join(dir_path, 'sample_data/user_anime/retrieval/model')
        )
        
        assert(retrieved_results.shape == (2,))

class Test3InferenceTest(unittest.TestCase):
    
    def test_inference(self):
        
        dir_path = os.path.dirname(os.path.realpath(__file__))

        model_path = os.path.join(dir_path, 'sample_data/user_anime/retrieval/model')
        input_data_path = os.path.join(dir_path, 'sample_data/user_anime/retrieval/infer')

        infer_data = infer_load_user_anime_retrieval(input_data_path, 'csv', 'tfds')
        infer_data = infer_data.batch(16)

        model = tf.saved_model.load(model_path)

        local_infer_result_path = infer_user_anime_retrieval(infer_data, model)

        results = pd.read_csv(f"{local_infer_result_path}/result_0.csv")
        
        results['anime_id'] = results['anime_id'].apply(str)
        results = results[results['user_id'] == 'Dedzapadlo']['anime_id'].values
        results = list(results)

        _, results_2 = model(tf.constant(['Dedzapadlo']))
        results_2 = results_2.numpy()[0]
        results_2 = list(map(lambda x : x.decode('utf-8'), results_2))

        self.assertCountEqual(
            results,
            results_2
        )