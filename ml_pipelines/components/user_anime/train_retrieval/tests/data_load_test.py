import os
import sys
import unittest

import tensorflow as tf

sys.path.append(os.path.abspath(__file__ + "/../../"))

from src.data_load import load_user_anime_retrieval_dataset

class DataLoadTest(unittest.TestCase):

    def test_user_anime_retrieval_dataload(self):

        dir_path = os.path.dirname(os.path.realpath(__file__))
        data_path = os.path.join(dir_path, 'sample_data/train.csv')

        dataset = load_user_anime_retrieval_dataset(data_path, batch_size=16, shuffle=False)

        assert('user_id' in dataset.element_spec)
        assert('anime_id' in dataset.element_spec)
        assert(len(dataset.element_spec.keys()) == 2)

        assert(dataset.element_spec['user_id'].dtype == tf.string)
        assert(dataset.element_spec['anime_id'].dtype == tf.string)

        for x in dataset.take(1):
            assert(x['user_id'].shape == (16,))
            assert(x['anime_id'].shape == (16,))

    def test_no_shuffle(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        data_path = os.path.join(dir_path, 'sample_data/train.csv')

        dataset = load_user_anime_retrieval_dataset(data_path, batch_size=16, shuffle=False)
        for x in dataset.take(1):
            batch_1 = x
        for x in dataset.take(1):
            batch_2 = x

        tf.test.TestCase().assertAllEqual(a=batch_1['user_id'], b=batch_2['user_id'])
        tf.test.TestCase().assertAllEqual(a=batch_1['anime_id'], b=batch_2['anime_id'])

    def test_shuffle(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        data_path = os.path.join(dir_path, 'sample_data/train.csv')

        dataset = load_user_anime_retrieval_dataset(data_path, batch_size=16, shuffle=True)
        for x in dataset.take(1):
            batch_1 = x
        for x in dataset.take(1):
            batch_2 = x

        tf.test.TestCase().assertNotAllEqual(a=batch_1['user_id'], b=batch_2['user_id'])
        tf.test.TestCase().assertNotAllEqual(a=batch_1['anime_id'], b=batch_2['anime_id'])
