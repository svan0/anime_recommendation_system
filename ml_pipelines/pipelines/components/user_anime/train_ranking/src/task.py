import argparse
import json


import tensorflow as tf

from ml_package.utils.data_load_utils import load_list_anime_np
from ml_package.utils.data_load_utils import load_list_user_np

from data_load import load_user_anime_list_ranking_dataset
from data_load import load_user_anime_rating_prediction_dataset

from train import train_user_anime_list_ranking_model
from train import train_user_anime_rating_prediction_model

from ml_package.utils.evaluation_utils import evaluate_model_all_datasets

from ml_package.utils.gcs_utils import write_json_to_gcs


def run_task(
        model_type,
        train_data_path,
        val_data_path,
        test_data_path,
        all_anime_data_path,
        all_user_data_path,
        model_path,
        metrics_path,
        hyperparameters
):

    list_anime = load_list_anime_np(all_anime_data_path)
    list_user = load_list_user_np(all_user_data_path)

    if model_type == 'list_ranking':
        train_data = load_user_anime_list_ranking_dataset(
            train_data_path,
            batch_size=2048,
            shuffle=True
        )
        val_data = load_user_anime_list_ranking_dataset(
            val_data_path,
            batch_size=2048,
            shuffle=False
        )
        test_data = load_user_anime_list_ranking_dataset(
            test_data_path,
            batch_size=2048,
            shuffle=False
        )

        ranking_model = train_user_anime_list_ranking_model(
            list_anime,
            list_user,
            train_data,
            val_data,
            hyperparameters
        )

        _ = ranking_model({
            'user_id' : tf.constant([[list_user[0]]]),
            'anime_id' : tf.constant([[list_anime[0]]]),
        })
    else:
        train_data = load_user_anime_rating_prediction_dataset(
            train_data_path,
            batch_size=2048,
            shuffle=True
        )
        val_data = load_user_anime_rating_prediction_dataset(
            val_data_path,
            batch_size=2048,
            shuffle=False
        )
        test_data = load_user_anime_rating_prediction_dataset(
            test_data_path,
            batch_size=2048,
            shuffle=False
        )

        ranking_model = train_user_anime_rating_prediction_model(
            list_anime,
            list_user,
            train_data,
            val_data,
            hyperparameters
        )

        _ = ranking_model({
            'user_id' : tf.constant([list_user[0]]),
            'anime_id' : tf.constant([list_anime[0]]),
        })

    ranking_model.save(model_path)
    
    metrics = evaluate_model_all_datasets(ranking_model, train_data, val_data, test_data)

    if metrics_path.startswith('/gcs/'):
        write_json_to_gcs(metrics, metrics_path)
    else:
        with open(metrics_path, "w") as file:
            json.dump(metrics, file, indent=4, sort_keys=True)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Train and evaluate user anime ranking model')

    parser.add_argument('--model-type', type = str)
    parser.add_argument('--train-data-path', type = str)
    parser.add_argument('--val-data-path', type = str)
    parser.add_argument('--test-data-path', type = str)
    parser.add_argument('--all-anime-data-path', type = str)
    parser.add_argument('--all-user-data-path', type = str)
    parser.add_argument('--output-model-path', type = str)
    parser.add_argument('--output-metrics-path', type = str)
    parser.add_argument('--anime-embedding-size', type = int)
    parser.add_argument('--user-embedding-size', type = int)
    parser.add_argument('--scoring-layer-size', type = int)
    parser.add_argument('--learning-rate', type = float)
    parser.add_argument('--optimizer', type = str)
    parser.add_argument('--max-num-epochs', type = int)
    parser.add_argument('--early-stop-num-epochs', type = int)

    args = parser.parse_args()

    hyperparameters = {
        'anime_embedding_size' : args.anime_embedding_size,
        'user_embedding_size' : args.user_embedding_size,
        'scoring_layer_size' : args.scoring_layer_size,
        'learning_rate' : args.learning_rate,
        'optimizer' : args.optimizer,
        'max_num_epochs' : args.max_num_epochs,
        'early_stop_num_epochs' : args.early_stop_num_epochs
    }

    run_task(
        args.model_type,
        args.train_data_path,
        args.val_data_path,
        args.test_data_path,
        args.all_anime_data_path,
        args.all_user_data_path,
        args.output_model_path,
        args.output_metrics_path,
        hyperparameters
    )
