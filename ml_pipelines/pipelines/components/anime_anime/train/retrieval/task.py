import tensorflow as tf
import argparse
import logging
import json
import tempfile

from anime_rec.data.data_loaders.common.anime import load_list_anime

from anime_rec.data.data_loaders.train.anime_anime import load_anime_anime_retrieval
from anime_rec.train.anime_anime import train_anime_anime_retrieval_model, evaluate_anime_anime_retrieval_model, get_anime_anime_retrieval_index

from anime_rec.data.data_loaders.gcs_utils import upload_local_folder_to_gcs

if __name__ == '__main__':
    
    logging.basicConfig(
        format='%(levelname)s: %(asctime)s: %(message)s',
        level=logging.INFO
    )

    parser = argparse.ArgumentParser(description='Train and evaluate anime anime retrieval model')
    parser.add_argument('--data-format', type = str)
    parser.add_argument('--train-data-path', type = str)
    parser.add_argument('--val-data-path', type = str)
    parser.add_argument('--test-data-path', type = str)
    parser.add_argument('--anime-data-path', type = str)
    parser.add_argument('--model-path', type = str)
    parser.add_argument('--metrics-path', type = str)
    parser.add_argument('--anime-embedding-size', type = int)
    parser.add_argument('--learning-rate', type = float)
    parser.add_argument('--optimizer', type = str)
    parser.add_argument('--max-num-epochs', type = int)
    parser.add_argument('--early-stop-num-epochs', type = int)
    args = parser.parse_args()

    train_data_path = args.train_data_path
    val_data_path = args.val_data_path
    test_data_path = args.test_data_path
    anime_data_path = args.anime_data_path
    data_format = args.data_format
    model_path = args.model_path
    metrics_path = args.metrics_path

    hyperparameters = {
        'anime_embedding_size' : args.anime_embedding_size,
        'learning_rate' : args.learning_rate,
        'optimizer' : args.optimizer,
        'max_num_epochs' : args.max_num_epochs,
        'early_stop_num_epochs' : args.early_stop_num_epochs
    }

    train_data = load_anime_anime_retrieval(train_data_path, data_format, 'tfds')
    train_data = train_data.shuffle(100_000, reshuffle_each_iteration=True)
    train_data = train_data.batch(2048).prefetch(4).cache()

    validation_data = load_anime_anime_retrieval(val_data_path, data_format, 'tfds')
    validation_data = validation_data.batch(2048).prefetch(4).cache()

    test_data = load_anime_anime_retrieval(test_data_path, data_format, 'tfds')
    test_data = test_data.batch(2048).prefetch(4).cache()

    anime_data = load_list_anime(anime_data_path, data_format, 'numpy')
    
    model = train_anime_anime_retrieval_model(anime_data, train_data, validation_data, hyperparameters)
    
    retrieval_model_index = get_anime_anime_retrieval_index(anime_data, model, k = 300)
     
    _ = retrieval_model_index(tf.constant([anime_data[0]]))
    
    tf.saved_model.save(
        retrieval_model_index,
        model_path
    )

    metrics = evaluate_anime_anime_retrieval_model(model, train_data, validation_data, test_data)
    metrics_folder = tempfile.mkdtemp()
    with open(f"{metrics_folder}/metrics.json", 'w') as f:
        f.write(json.dumps(metrics, indent=4, sort_keys=True))
    upload_local_folder_to_gcs(metrics_folder, metrics_path)
