import argparse
import tensorflow as tf
import logging

from anime_rec.data.data_loaders.infer.user_anime import load_user_anime_retrieval
from anime_rec.infer.user_anime import infer_user_anime_retrieval
from anime_rec.data.data_loaders.gcs_utils import upload_local_folder_to_gcs


if __name__=='__main__':
    
    logging.basicConfig(
        format='%(levelname)s: %(asctime)s: %(message)s',
        level=logging.INFO
    )

    parser = argparse.ArgumentParser(description = 'Infer user anime ranking model')
    parser.add_argument('--model-path', type = str)
    parser.add_argument('--input-data-path', type = str)
    parser.add_argument('--output-data-path', type = str)

    args = parser.parse_args()

    model_path = args.model_path
    input_data_path = args.input_data_path
    output_data_path = args.output_data_path

    model = tf.saved_model.load(model_path)

    infer_data = load_user_anime_retrieval(input_data_path, 'csv', 'tfds')
    infer_data = infer_data.batch(2048)
    local_infer_result_path = infer_user_anime_retrieval(infer_data, model)
    upload_local_folder_to_gcs(local_infer_result_path, output_data_path)
    
