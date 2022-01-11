import os
import sys
import unittest

import tensorflow as tf

sys.path.append(os.path.abspath(__file__ + "/../../"))

from src.data_load import load_anime_anime_pair_classification_dataset

class DataLoadTest(unittest.TestCase):

    def test_user_anime_retrieval_dataload(self):

        dir_path = os.path.dirname(os.path.realpath(__file__))
        data_path = os.path.join(dir_path, 'sample_data/train.csv')

        dataset = load_anime_anime_pair_classification_dataset(data_path, batch_size=16, shuffle=False)

        print(dataset.element_spec)
        
        assert(type(dataset.element_spec) == tuple)
        assert(len(dataset.element_spec) == 2)

        assert(dataset.element_spec[1].dtype == tf.int32)

        assert('anchor_anime' in dataset.element_spec[0])
        assert('rel_anime_1' in dataset.element_spec[0])
        assert('rel_anime_2' in dataset.element_spec[0])
        assert(len(dataset.element_spec[0].keys()) == 3)

        assert(dataset.element_spec[0]['anchor_anime'].dtype == tf.string)
        assert(dataset.element_spec[0]['rel_anime_1'].dtype == tf.string)
        assert(dataset.element_spec[0]['rel_anime_2'].dtype == tf.string)

        for x in dataset.take(1):
            assert(x[0]['anchor_anime'].shape == (16,))
            assert(x[0]['rel_anime_1'].shape == (16,))
            assert(x[0]['rel_anime_2'].shape == (16,))

    def test_no_shuffle(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        data_path = os.path.join(dir_path, 'sample_data/train.csv')

        dataset = load_anime_anime_pair_classification_dataset(data_path, batch_size=16, shuffle=False)
        for x in dataset.take(1):
            batch_1 = x
        for x in dataset.take(1):
            batch_2 = x

        tf.test.TestCase().assertAllEqual(a=batch_1[0]['anchor_anime'], b=batch_2[0]['anchor_anime'])
        tf.test.TestCase().assertAllEqual(a=batch_1[0]['rel_anime_1'], b=batch_2[0]['rel_anime_1'])
        tf.test.TestCase().assertAllEqual(a=batch_1[0]['rel_anime_2'], b=batch_2[0]['rel_anime_2'])
        tf.test.TestCase().assertAllEqual(a=batch_1[1], b=batch_2[1])

    def test_shuffle(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        data_path = os.path.join(dir_path, 'sample_data/train.csv')

        dataset = load_anime_anime_pair_classification_dataset(data_path, batch_size=16, shuffle=True)
        for x in dataset.take(1):
            batch_1 = x
        for x in dataset.take(1):
            batch_2 = x

        tf.test.TestCase().assertNotAllEqual(a=batch_1[0]['anchor_anime'], b=batch_2[0]['anchor_anime'])
        tf.test.TestCase().assertNotAllEqual(a=batch_1[0]['rel_anime_1'], b=batch_2[0]['rel_anime_1'])
        tf.test.TestCase().assertNotAllEqual(a=batch_1[0]['rel_anime_2'], b=batch_2[0]['rel_anime_2'])
