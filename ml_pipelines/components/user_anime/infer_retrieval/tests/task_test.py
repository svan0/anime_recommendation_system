import os
import sys
import unittest
import pandas as pd
import tensorflow as tf

sys.path.append(os.path.abspath(__file__ + "/../../"))

from src.task import run_task

class TaskTest(unittest.TestCase):

    def test_task_runs(self):

        dir_path = os.path.dirname(os.path.realpath(__file__))

        model_path = os.path.join(dir_path, 'sample_data/trained_model')
        input_data_path = os.path.join(dir_path, 'sample_data/infer.csv')
        output_data_path = os.path.join(dir_path, 'sample_data/results.csv')

        run_task(
            model_path,
            input_data_path,
            output_data_path
        )

        
        results = pd.read_csv(output_data_path)
        results['retrieved_anime_id'] = results['retrieved_anime_id'].apply(str)
        results = results[results['user_id'] == 'Dedzapadlo']['retrieved_anime_id'].values
        results = list(results)

        model = tf.saved_model.load(model_path)
        _, results_2 = model(tf.constant(['Dedzapadlo']))
        results_2 = results_2.numpy()[0]
        results_2 = list(map(lambda x : x.decode('utf-8'), results_2))

        self.assertCountEqual(
            results,
            results_2
        )
        