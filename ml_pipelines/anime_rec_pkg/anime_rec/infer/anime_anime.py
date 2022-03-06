import tempfile
import logging
import pandas as pd
import tensorflow as tf

'''
    Anime Anime Retrieval Inference
'''
def infer_anime_anime_retrieval(infer_data, model):
    local_data_folder = tempfile.mkdtemp()
    for batch_idx, batch in enumerate(infer_data):
        
        logging.info(f"Running anime anime retrieval inference on batch {batch_idx}")
        
        scores, animes = model(batch['anime_id'])
        df = pd.DataFrame({
            'anime_id' : list(batch['anime_id'].numpy()),
            'retrieved_anime_id' : list(animes.numpy()),
        })
        df = df.explode('retrieved_anime_id')
        df['anime_id'] = df['anime_id'].apply(lambda x : x.decode('utf-8'))
        df['retrieved_anime_id'] = df['retrieved_anime_id'].apply(lambda x : x.decode('utf-8'))

        df.to_csv(f"{local_data_folder}/result_{batch_idx}.csv", index=False)
        
        logging.info(f"Saved anime anime retrieval inference on batch to '{local_data_folder}/result_{batch_idx}.csv'")
    
    return local_data_folder

'''
    Anime Anime Ranking Inference
'''
def infer_anime_anime_ranking(infer_data, model):
    local_data_folder = tempfile.mkdtemp()
    for batch_idx, batch in enumerate(infer_data):
        
        logging.info(f"Running anime anime ranking inference on batch {batch_idx}")
        
        df = pd.DataFrame({
            'anime_id' : list(tf.squeeze(batch['anime_id']).numpy()),
            'retrieved_anime_id' : list(tf.squeeze(batch['retrieved_anime_id']).numpy()),
            'score' : list(tf.squeeze(model(batch)).numpy())
        })
        df['anime_id'] = df['anime_id'].apply(lambda x : x.decode('utf-8'))
        df['retrieved_anime_id'] = df['retrieved_anime_id'].apply(lambda x : x.decode('utf-8'))
        df.to_csv(f"{local_data_folder}/result_{batch_idx}.csv", index = False)

        logging.info(f"Saved anime anime ranking inference on batch to '{local_data_folder}/result_{batch_idx}.csv'")
        
    return local_data_folder
