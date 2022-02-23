import argparse
import numpy as np
import pandas as pd
import tensorflow as tf


from ml_package.models.user_anime_ranking_models import UserAnimeRatingPredictionModel, UserAnimeListRankingModel
from ml_package.utils.gcs_utils import gcs_create_empty_folder

def run_task(
    model_type,
    model_path,
    input_data_path,
    output_data_path
    ):

    if model_type == 'list_ranking':
        model = tf.keras.models.load_model(
            model_path,
            custom_objects={
                'UserAnimeListRankingModel' : UserAnimeListRankingModel
            }
        )
    else:
        model = tf.keras.models.load_model(
            model_path,
            custom_objects={
                'UserAnimeListRankingModel' : UserAnimeRatingPredictionModel
            }
        )

    inference_data = pd.read_csv(input_data_path, keep_default_na=False)
    inference_data['user_id'] = inference_data['user_id'].apply(str)
    inference_data['retrieved_anime_id'] = inference_data['retrieved_anime_id'].apply(str)

    anime_inference_ds = tf.data.Dataset.from_tensor_slices(
            {
                'user_id' : tf.expand_dims(inference_data['user_id'], -1),
                'anime_id' : tf.expand_dims(tf.cast(inference_data['retrieved_anime_id'], tf.string), -1)
            }
    )
    anime_inference_ds = anime_inference_ds.batch(2048)

    results = []
    for x in anime_inference_ds:
        results.append(model(x)[:, 0])

    results = np.concatenate(results)

    inference_data['ranking_score'] = list(results)

    if output_data_path.startswith('/gcs/'):
        gcs_create_empty_folder(output_data_path)

    inference_data.to_csv(output_data_path, index = False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'Infer user anime ranking model')
    parser.add_argument('--model-type', type = str)
    parser.add_argument('--model-path', type = str)
    parser.add_argument('--input-data-path', type = str)
    parser.add_argument('--output-data-path', type = str)

    args = parser.parse_args()
    
    run_task(
        args.model_type,
        args.model_path,
        args.input_data_path,
        args.output_data_path
    )
