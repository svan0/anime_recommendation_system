import argparse
import sys
import os
import json
import tensorflow as tf

sys.path.append(os.path.abspath(__file__ + "/../../../../../"))
sys.path.append(os.path.abspath(__file__ + "/../"))

from utils.data_load_utils import load_list_anime_np
from utils.data_load_utils import load_list_user_np
from data_load import load_user_anime_retrieval_dataset

from train import train_user_anime_retrieval_model
from train import get_user_anime_retrieval_index

from utils.evaluation_utils import evaluate_model_all_datasets

from utils.gcs_utils import write_json_to_gcs

def run_task(
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

        train_data = load_user_anime_retrieval_dataset(
            train_data_path,
            batch_size=2048,
            shuffle=True
        )
        val_data = load_user_anime_retrieval_dataset(
            val_data_path,
            batch_size=2048,
            shuffle=False
        )
        test_data = load_user_anime_retrieval_dataset(
            test_data_path,
            batch_size=2048,
            shuffle=False
        )
        
        retrieval_model = train_user_anime_retrieval_model(
            list_anime,
            list_user,
            train_data,
            val_data,
            hyperparameters
        )
        
        metrics = evaluate_model_all_datasets(retrieval_model, train_data, val_data, test_data)

        if metrics_path.startswith('/gcs/'):
            write_json_to_gcs(metrics, metrics_path)
        else:
            os.makedirs(os.path.dirname(metrics_path), exist_ok=True)
            with open(metrics_path, "w") as f:
                json.dump(metrics, f, indent=4, sort_keys=True)


        retrieval_model_index = get_user_anime_retrieval_index(list_anime, retrieval_model)

        # Need to call the index to set the shapes. TODO look for a better way to handle this
        _ = retrieval_model_index(tf.constant([list_user[0]]))
        
        tf.saved_model.save(
            retrieval_model_index,
            model_path
        )

if __name__ == '__main__':

        parser = argparse.ArgumentParser(description='Train and evaluate user anime retrieval model')

        parser.add_argument('--train-data-path', type = str)
        parser.add_argument('--val-data-path', type = str)
        parser.add_argument('--test-data-path', type = str)
        parser.add_argument('--all-anime-data-path', type = str)
        parser.add_argument('--all-user-data-path', type = str)
        parser.add_argument('--output-model-path', type = str)
        parser.add_argument('--output-metrics-path', type = str)
        parser.add_argument('--user-anime-embedding-size', type = int)
        parser.add_argument('--learning-rate', type = float)
        parser.add_argument('--optimizer', type = str)
        parser.add_argument('--max-num-epochs', type = int)
        parser.add_argument('--early-stop-num-epochs', type = int)


        args = parser.parse_args()
        
        hyperparameters = {
            'user_anime_embedding_size' : args.user_anime_embedding_size,
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
            args.all_user_data_path,
            args.output_model_path,
            args.output_metrics_path,
            hyperparameters
        )