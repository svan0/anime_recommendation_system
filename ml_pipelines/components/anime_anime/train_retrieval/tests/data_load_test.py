import os
import sys
import unittest

import tensorflow as tf

sys.path.append(os.path.abspath(__file__ + "/../../"))

from src.data_load import load_anime_anime_retrieval_dataset

class DataLoadTest(unittest.TestCase):

    def test_user_anime_retrieval_dataload(self):

        dir_path = os.path.dirname(os.path.realpath(__file__))
        data_path = os.path.join(dir_path, 'sample_data/train.csv')

        dataset = load_anime_anime_retrieval_dataset(data_path, batch_size=16, shuffle=False)

        assert('animeA' in dataset.element_spec)
        assert('animeB' in dataset.element_spec)
        assert(len(dataset.element_spec.keys()) == 2)

        assert(dataset.element_spec['animeA'].dtype == tf.string)
        assert(dataset.element_spec['animeB'].dtype == tf.string)

        for x in dataset.take(1):
            assert(x['animeA'].shape == (16,))
            assert(x['animeB'].shape == (16,))

    def test_no_shuffle(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        data_path = os.path.join(dir_path, 'sample_data/train.csv')

        dataset = load_anime_anime_retrieval_dataset(data_path, batch_size=16, shuffle=False)
        for x in dataset.take(1):
            batch_1 = x
        for x in dataset.take(1):
            batch_2 = x

        tf.test.TestCase().assertAllEqual(a=batch_1['animeA'], b=batch_2['animeA'])
        tf.test.TestCase().assertAllEqual(a=batch_1['animeB'], b=batch_2['animeB'])

    def test_shuffle(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        data_path = os.path.join(dir_path, 'sample_data/train.csv')

        dataset = load_anime_anime_retrieval_dataset(data_path, batch_size=16, shuffle=True)
        for x in dataset.take(1):
            batch_1 = x
        for x in dataset.take(1):
            batch_2 = x

        tf.test.TestCase().assertNotAllEqual(a=batch_1['animeA'], b=batch_2['animeA'])
        tf.test.TestCase().assertNotAllEqual(a=batch_1['animeB'], b=batch_2['animeB'])
