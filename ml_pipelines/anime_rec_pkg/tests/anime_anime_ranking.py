import os
import unittest
import pandas as pd
import tensorflow as tf

from anime_rec.data.data_loaders.common.anime import load_list_anime
from anime_rec.data.data_loaders.train.anime_anime import load_anime_anime_pair_classification_ranking
from anime_rec.data.data_loaders.infer.anime_anime import load_anime_anime_ranking

from anime_rec.train.anime_anime import get_anime_anime_pair_ranking_classification_model, train_anime_anime_pair_ranking_classification_model

from anime_rec.infer.anime_anime import infer_anime_anime_ranking

class Test1DataLoadTest(unittest.TestCase):

    def test_user_anime_ranking_dataload(self):

        dir_path = os.path.dirname(os.path.realpath(__file__))
        data_path = os.path.join(dir_path, 'sample_data/anime_anime/ranking/train')

        dataset = load_anime_anime_pair_classification_ranking(data_path, 'csv', 'tfds').batch(16)
        dataset = dataset.map(lambda x : ({
                'anime_id' : x['anime_id'],
                'retrieved_anime_id_1' : x['retrieved_anime_id_1'],
                'retrieved_anime_id_2' : x['retrieved_anime_id_2']
            },
            x['label']
        ))
        
        assert(type(dataset.element_spec) == tuple)
        assert(len(dataset.element_spec) == 2)

        assert(dataset.element_spec[1].dtype == tf.int32)

        assert('anime_id' in dataset.element_spec[0])
        assert('retrieved_anime_id_1' in dataset.element_spec[0])
        assert('retrieved_anime_id_2' in dataset.element_spec[0])
        assert(len(dataset.element_spec[0].keys()) == 3)

        assert(dataset.element_spec[0]['anime_id'].dtype == tf.string)
        assert(dataset.element_spec[0]['retrieved_anime_id_1'].dtype == tf.string)
        assert(dataset.element_spec[0]['retrieved_anime_id_2'].dtype == tf.string)

        for x in dataset.take(1):
            assert(x[0]['anime_id'].shape == (16,))
            assert(x[0]['retrieved_anime_id_1'].shape == (16,))
            assert(x[0]['retrieved_anime_id_2'].shape == (16,))

class Test2TrainingTest(unittest.TestCase):

    def test_weights_updated(self):

        dir_path = os.path.dirname(os.path.realpath(__file__))

        all_anime_data_path = os.path.join(dir_path, 'sample_data/list_anime')
        train_data_path = os.path.join(dir_path, 'sample_data/anime_anime/ranking/train')

        list_anime = load_list_anime(all_anime_data_path, 'csv', 'numpy')
        train_dataset = load_anime_anime_pair_classification_ranking(train_data_path, 'csv', 'tfds').batch(2048)
        train_dataset = train_dataset.map(lambda x : ({
                'anime_id' : x['anime_id'],
                'retrieved_anime_id_1' : x['retrieved_anime_id_1'],
                'retrieved_anime_id_2' : x['retrieved_anime_id_2']
            },
            x['label']
        ))
        model = get_anime_anime_pair_ranking_classification_model(
            list_anime,
            128,
            128,
            0.005,
            'adam'
        )

        anime_embeddings_1 = model.anime_scoring_model.anime_model.anime_embedding_layer.get_weights()

        model.fit(train_dataset, epochs = 1, verbose = 0)

        anime_embeddings_2 = model.anime_scoring_model.anime_model.anime_embedding_layer.get_weights()

        tf.test.TestCase().assertNotAllEqual(a=anime_embeddings_1, b=anime_embeddings_2)

    def test_one_batch_overfit(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))

        all_anime_data_path = os.path.join(dir_path, 'sample_data/list_anime')
        train_data_path = os.path.join(dir_path, 'sample_data/anime_anime/ranking/train')
        validation_data_path = os.path.join(dir_path, 'sample_data/anime_anime/ranking/val')

        list_anime = load_list_anime(all_anime_data_path, 'csv', 'numpy')
        
        train_dataset = load_anime_anime_pair_classification_ranking(train_data_path, 'csv', 'tfds').batch(16).take(1)
        train_dataset = train_dataset.map(lambda x : ({
                'anime_id' : x['anime_id'],
                'retrieved_anime_id_1' : x['retrieved_anime_id_1'],
                'retrieved_anime_id_2' : x['retrieved_anime_id_2']
            },
            x['label']
        ))

        val_dataset = load_anime_anime_pair_classification_ranking(validation_data_path, 'csv', 'tfds').batch(16).take(1)
        val_dataset = val_dataset.map(lambda x : ({
                'anime_id' : x['anime_id'],
                'retrieved_anime_id_1' : x['retrieved_anime_id_1'],
                'retrieved_anime_id_2' : x['retrieved_anime_id_2']
            },
            x['label']
        ))

        hyperparameters = {
            'anime_embedding_size' : 128,
            'scoring_layer_size' : 128,
            'learning_rate' : 0.005,
            'optimizer' : 'adam',
            'early_stop_num_epochs' : 100,
            'max_num_epochs' : 100
        }

        model = train_anime_anime_pair_ranking_classification_model(
            list_anime,
            train_dataset,
            val_dataset,
            hyperparameters
        )

        train_metrics = model.evaluate(train_dataset, return_dict=True)
        val_metrics = model.evaluate(val_dataset, return_dict=True)

        tf.saved_model.save(
            model.anime_scoring_model,
            os.path.join(dir_path, 'sample_data/anime_anime/ranking/model')
        )

        assert(train_metrics['binary_accuracy'] == 1.0)
        assert(train_metrics['precision'] == 1.0)
        assert(train_metrics['recall'] == 1.0)
        assert(val_metrics['loss'] > 10.0 * train_metrics['loss'])

class Test3InferenceTest(unittest.TestCase):
    
    def test_inference(self):
        
        dir_path = os.path.dirname(os.path.realpath(__file__))

        model_path = os.path.join(dir_path, 'sample_data/anime_anime/ranking/model')
        input_data_path = os.path.join(dir_path, 'sample_data/anime_anime/ranking/infer')

        infer_data = load_anime_anime_ranking(input_data_path, 'csv', 'tfds')
        infer_data = infer_data.batch(16)

        model = tf.saved_model.load(model_path)

        local_infer_result_path = infer_anime_anime_ranking(infer_data, model)

        results = pd.read_csv(f"{local_infer_result_path}/result_0.csv")

        results['anime_id'] = results['anime_id'].apply(str)
        results['retrieved_anime_id'] = results['retrieved_anime_id'].apply(str)
        results = results[(results['anime_id'] == '32281') & (results['retrieved_anime_id'] == '889')]['score'].values[0]

        results_2 = model({
            'anime_id' : tf.constant([['32281']]),
            'retrieved_anime_id' : tf.constant([['889']])
        })
        results_2 = results_2.numpy()[0, 0, 0]

        self.assertAlmostEquals(results, results_2, places = 4)