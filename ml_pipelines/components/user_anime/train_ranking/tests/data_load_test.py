import os
import sys
import unittest

import tensorflow as tf

sys.path.append(os.path.abspath(__file__ + "/../../"))

from src.data_load import load_user_anime_list_ranking_dataset

class DataLoadTest(unittest.TestCase):
    def test_user_anime_list_ranking_dataload(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        data_path = os.path.join(dir_path, 'sample_data/train.csv')

        dataset = load_user_anime_list_ranking_dataset(data_path, batch_size=16, shuffle=False)

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
    
    def test_no_shuffle(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        data_path = os.path.join(dir_path, 'sample_data/train.csv')

        dataset = load_user_anime_list_ranking_dataset(data_path, batch_size=16, shuffle=False)
        for x in dataset.take(1):
            batch_1 = x
        for x in dataset.take(1):
            batch_2 = x

        tf.test.TestCase().assertAllEqual(a=batch_1[0]['user_id'], b=batch_2[0]['user_id'])
        tf.test.TestCase().assertAllEqual(a=batch_1[0]['anime_id'], b=batch_2[0]['anime_id'])
        tf.test.TestCase().assertAllEqual(a=batch_1[1], b=batch_2[1])
    
    def test_shuffle(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        data_path = os.path.join(dir_path, 'sample_data/train.csv')

        dataset = load_user_anime_list_ranking_dataset(data_path, batch_size=16, shuffle=True)
        for x in dataset.take(1):
            batch_1 = x
        for x in dataset.take(1):
            batch_2 = x

        tf.test.TestCase().assertNotAllEqual(a=batch_1[0]['user_id'], b=batch_2[0]['user_id'])
        tf.test.TestCase().assertNotAllEqual(a=batch_1[0]['anime_id'], b=batch_2[0]['anime_id'])
        tf.test.TestCase().assertNotAllEqual(a=batch_1[1], b=batch_2[1])
        

