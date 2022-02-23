'''
    Define Anime models
    (models that take as input an anime_id (string) and
    return a vector embedding
    )
    TODO : implement an experiment with models that use
    anime synposis, title, genres, images ... to compute
    the vector embedding
'''
import tensorflow as tf

class SimpleAnimeModel(tf.keras.Model):
    '''
        Simple Anime model that only uses an embedding
        layer to map the anime_id to a vector
    '''
    def __init__(self, unique_anime_ids, anime_embedding_size=32):
        super().__init__()
        self.anime_id_lookup_layer = tf.keras.layers.StringLookup(
            vocabulary=unique_anime_ids, 
            num_oov_indices=0,
            name='simple_anime_model_id_lookup'
        )
        self.anime_embedding_layer = tf.keras.layers.Embedding(
            self.anime_id_lookup_layer.vocabulary_size(), 
            anime_embedding_size,
            name='simple_anime_model_embedding',
            trainable=True
        )

    def call(self, anime_id):
        '''
            Compute the anime embedding
        '''
        anime_idx = self.anime_id_lookup_layer(anime_id)
        anime_embedding = self.anime_embedding_layer(anime_idx)
        return anime_embedding
