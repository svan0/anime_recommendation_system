import argparse
import sys
import os
import json

sys.path.append(os.path.abspath(__file__ + "/../../../../"))
sys.path.append(os.path.abspath(__file__ + "/../"))

import tensorflow as tf

from utils.data_load_utils import load_list_anime_np
from data_load import load_anime_anime_pair_classification_dataset

from train import train_anime_anime_pair_ranking_classification_model

from utils.evaluation_utils import evaluate_model_all_datasets

from utils.gcs_utils import write_json_to_gcs

def run_task(
    train_data_path,
    val_data_path,
    test_data_path,
    all_anime_data_path,
    model_path,
    metrics_path,
    hyperparameters
):
    
    list_anime = load_list_anime_np(all_anime_data_path)
    
    train_data = load_anime_anime_pair_classification_dataset(
        train_data_path,
        batch_size=2048,
        shuffle=True
    )
    
    val_data = load_anime_anime_pair_classification_dataset(
        val_data_path,
        batch_size=2048,
        shuffle=False
    )
    
    test_data = load_anime_anime_pair_classification_dataset(
        test_data_path,
        batch_size=2048,
        shuffle=False
    )
    
    pair_classification_model = train_anime_anime_pair_ranking_classification_model(
        list_anime,
        train_data,
        val_data,
        hyperparameters
    )

    metrics = evaluate_model_all_datasets(pair_classification_model, train_data, val_data, test_data)
    if metrics_path.startswith('/gcs/'):
        write_json_to_gcs(metrics, metrics_path)
    else:
        with open(metrics_path, "w") as f:
            json.dump(metrics, f, indent=4, sort_keys=True)


    tf.saved_model.save(
        pair_classification_model.anime_scoring_model,
        model_path
    )


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Train anime anime ranking model')

    parser.add_argument('--train-data-path', type = str)
    parser.add_argument('--val-data-path', type = str)
    parser.add_argument('--test-data-path', type = str)
    parser.add_argument('--all-anime-data-path', type = str)
    parser.add_argument('--output-model-path', type = str)
    parser.add_argument('--output-metrics-path', type = str)
    parser.add_argument('--anime-embedding-size', type = int, default = 0)
    parser.add_argument('--scoring-layer-size', type = int, default = 0)
    parser.add_argument('--learning-rate', type = float, default = 0.0)
    parser.add_argument('--optimizer', type = str, default = 'None')
    parser.add_argument('--max-num-epochs', type = int)
    parser.add_argument('--early-stop-num-epochs', type = int)

    args = parser.parse_args()
    
    hyperparameters = {
        'anime_embedding_size' : args.anime_embedding_size,
        'scoring_layer_size' : args.scoring_layer_size,
        'learning_rate' : args.learning_rate,
        'optimizer' : args.optimizer,
        'max_num_epochs' : args.max_num_epochs,
        'early_stop_num_epochs' : args.early_stop_num_epochs
    }
    
    run_task(
        args.train_data_path,
        args.val_data_path,
        args.test_data_path,
        args.all_anime_data_path,
        args.output_model_path,
        args.output_metrics_path,
        hyperparameters
    )
