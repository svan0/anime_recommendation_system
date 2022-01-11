import os
import sys
import argparse
import numpy as np
import pandas as pd
import tensorflow as tf

sys.path.append(os.path.abspath(__file__ + "/../../../../../"))

from utils.gcs_utils import gcs_create_empty_folder

def run_task(
    model_path,
    input_data_path,
    output_data_path
):
    model = tf.saved_model.load(model_path)

    inference_data = pd.read_csv(input_data_path)
    inference_data['user_id'] = inference_data['user_id'].apply(str)

    anime_inference_ds = tf.data.Dataset.from_tensor_slices(inference_data['user_id'])
    anime_inference_ds = anime_inference_ds.batch(2048)

    results = []
    for x in anime_inference_ds:
        results.append(model(x)[1])
    results = np.concatenate(results)

    inference_data['retrieved_anime_id'] = list(results)
    inference_data = inference_data.explode('retrieved_anime_id')
    inference_data['retrieved_anime_id'] = inference_data['retrieved_anime_id'].apply(lambda x : x.decode("utf-8"))

    if output_data_path.startswith('/gcs/'):
        gcs_create_empty_folder(output_data_path)

    inference_data.to_csv(output_data_path, index = False)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description = 'Infer retrieval model')

    parser.add_argument('--model-path', type = str)
    parser.add_argument('--input-data-path', type = str)
    parser.add_argument('--output-data-path', type = str)

    args = parser.parse_args()

    run_task(
        args.model_path,
        args.input_data_path,
        args.output_data_path
    )
