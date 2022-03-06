import os
import unittest
import pandas as pd
import tensorflow as tf

from anime_rec.data.data_loaders.common.anime import load_list_anime
from anime_rec.data.data_loaders.common.user import load_list_user
from anime_rec.data.data_loaders.train.user_anime import load_user_anime_list_ranking as train_load_user_anime_list_ranking
from anime_rec.train.user_anime import get_user_anime_list_ranking_model, train_user_anime_list_ranking_model
from anime_rec.data.data_loaders.infer.user_anime import load_user_anime_list_ranking as infer_load_user_anime_list_ranking
from anime_rec.infer.user_anime import infer_user_anime_list_ranking

class Test1DataLoadTest(unittest.TestCase):

    def test_user_anime_list_ranking_dataload(self):

        dir_path = os.path.dirname(os.path.realpath(__file__))
        data_path = os.path.join(dir_path, 'sample_data/user_anime/list_ranking/train')

        dataset = train_load_user_anime_list_ranking(data_path, 'csv', 'tfds').batch(16)
        dataset = dataset.map(lambda x : (
            {
                'user_id' : x['user_id'],
                'anime_id' : x['anime_id']
            },
            x['score']
        ))
        assert(type(dataset.element_spec) == tuple)
        assert(len(dataset.element_spec) == 2)

        assert('user_id' in dataset.element_spec[0])
        assert('anime_id' in dataset.element_spec[0])
        assert(len(dataset.element_spec[0].keys()) == 2)

        assert(dataset.element_spec[0]['user_id'].dtype == tf.string)
        assert(dataset.element_spec[0]['anime_id'].dtype == tf.string)
        assert(dataset.element_spec[1].dtype == tf.float32)

        for x in dataset.take(1):
            assert(x[0]['user_id'].shape == (16,10))
            assert(x[0]['anime_id'].shape == (16,10))
            assert(x[1].shape == (16,10))

class Test2TrainingTest(unittest.TestCase):
    
    def test_weights_updated(self):

        dir_path = os.path.dirname(os.path.realpath(__file__))

        all_anime_data_path = os.path.join(dir_path, 'sample_data/list_anime')
        all_user_data_path = os.path.join(dir_path, 'sample_data/list_user')
        train_data_path = os.path.join(dir_path, 'sample_data/user_anime/list_ranking/train')

        list_anime = load_list_anime(all_anime_data_path, 'csv', 'numpy')
        list_user = load_list_user(all_user_data_path, 'csv', 'numpy')
        train_dataset = train_load_user_anime_list_ranking(train_data_path, 'csv', 'tfds').batch(2048)
        train_dataset = train_dataset.map(lambda x : (
            {
                'user_id' : x['user_id'],
                'anime_id' : x['anime_id']
            },
            x['score']
        ))
        model = get_user_anime_list_ranking_model(
            list_anime,
            list_user,
            128,
            128,
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
        train_data_path = os.path.join(dir_path, 'sample_data/user_anime/list_ranking/train')
        validation_data_path = os.path.join(dir_path, 'sample_data/user_anime/list_ranking/val')

        list_anime = load_list_anime(all_anime_data_path, 'csv', 'numpy')
        list_user = load_list_user(all_user_data_path, 'csv', 'numpy')
        train_dataset = train_load_user_anime_list_ranking(train_data_path, 'csv', 'tfds').batch(16).take(1)
        train_dataset = train_dataset.map(lambda x : (
            {
                'user_id' : x['user_id'],
                'anime_id' : x['anime_id']
            },
            x['score']
        ))
        val_dataset = train_load_user_anime_list_ranking(validation_data_path, 'csv', 'tfds').batch(16).take(1)
        val_dataset = val_dataset.map(lambda x : (
            {
                'user_id' : x['user_id'],
                'anime_id' : x['anime_id']
            },
            x['score']
        ))
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

        model.save(os.path.join(dir_path, 'sample_data/user_anime/list_ranking/model'))

        train_metrics = model.evaluate(train_dataset, return_dict=True)
        val_metrics = model.evaluate(val_dataset, return_dict=True)

        assert(val_metrics['loss'] > 3.0 * train_metrics['loss'])

class Test3InferenceTest(unittest.TestCase):
    
    def test_inference(self):
        
        dir_path = os.path.dirname(os.path.realpath(__file__))

        model_path = os.path.join(dir_path, 'sample_data/user_anime/ranking/model')
        input_data_path = os.path.join(dir_path, 'sample_data/user_anime/list_ranking/infer')

        infer_data = infer_load_user_anime_list_ranking(input_data_path, 'csv', 'tfds')
        infer_data = infer_data.batch(16)

        model = tf.saved_model.load(model_path)

        local_infer_result_path = infer_user_anime_list_ranking(infer_data, model)

        results = pd.read_csv(f"{local_infer_result_path}/result_0.csv")

        results['anime_id'] = results['anime_id'].apply(str)
        results = results[(results['user_id'] == 'spacecowboy') & (results['anime_id'] == '270')]['score'].values[0]

        results_2 = model({
            'user_id' : tf.constant([['spacecowboy']]),
            'anime_id' : tf.constant([['270']])
        })
        results_2 = results_2.numpy()[0, 0, 0]

        self.assertAlmostEquals(results, results_2, places = 4)