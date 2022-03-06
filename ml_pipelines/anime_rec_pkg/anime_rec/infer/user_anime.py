import tempfile
import logging
import pandas as pd
import tensorflow as tf

'''
    User Anime Retrieval Inference
'''
def infer_user_anime_retrieval(infer_data, model):
    local_data_folder = tempfile.mkdtemp()
    for batch_idx, batch in enumerate(infer_data):
        
        logging.info(f"Running user anime retrieval inference on batch {batch_idx}")
        
        scores, animes = model(batch['user_id'])
        df = pd.DataFrame({
            'user_id' : list(batch['user_id'].numpy()),
            'anime_id' : list(animes.numpy()),
        })
        df = df.explode('anime_id')
        df['user_id'] = df['user_id'].apply(lambda x : x.decode('utf-8'))
        df['anime_id'] = df['anime_id'].apply(lambda x : x.decode('utf-8'))

        df.to_csv(f"{local_data_folder}/result_{batch_idx}.csv", index=False)
        
        logging.info(f"Saved user anime retrieval inference on batch to '{local_data_folder}/result_{batch_idx}.csv'")
    
    return local_data_folder

'''
    User Anime Ranking Inference
'''
def infer_user_anime_ranking(infer_data, model):
    local_data_folder = tempfile.mkdtemp()
    for batch_idx, batch in enumerate(infer_data):
        
        logging.info(f"Running user anime ranking inference on batch {batch_idx}")
        
        df = pd.DataFrame({
            'user_id' : list(tf.squeeze(batch['user_id']).numpy()),
            'anime_id' : list(tf.squeeze(batch['anime_id']).numpy()),
            'score' : list(tf.squeeze(model(batch)).numpy())
        })
        df['user_id'] = df['user_id'].apply(lambda x : x.decode('utf-8'))
        df['anime_id'] = df['anime_id'].apply(lambda x : x.decode('utf-8'))
        df.to_csv(f"{local_data_folder}/result_{batch_idx}.csv", index = False)

        logging.info(f"Saved user anime ranking inference on batch to '{local_data_folder}/result_{batch_idx}.csv'")
        
    return local_data_folder

'''
    User Anime List Ranking Inference
'''
def infer_user_anime_list_ranking(infer_data, model):
    local_data_folder = tempfile.mkdtemp()
    for batch_idx, batch in enumerate(infer_data):
        
        logging.info(f"Running user anime list ranking inference on batch {batch_idx}")
        
        df = pd.DataFrame({
            'user_id' : list(tf.squeeze(batch['user_id']).numpy()),
            'anime_id' : list(tf.squeeze(batch['anime_id']).numpy()),
            'score' : list(tf.squeeze(model(batch)).numpy())
        })
        df['user_id'] = df['user_id'].apply(lambda x : x.decode('utf-8'))
        df['anime_id'] = df['anime_id'].apply(lambda x : x.decode('utf-8'))
        df.to_csv(f"{local_data_folder}/result_{batch_idx}.csv", index = False)

        logging.info(f"Saved user anime list ranking inference on batch to '{local_data_folder}/result_{batch_idx}.csv'")
    
    return local_data_folder

