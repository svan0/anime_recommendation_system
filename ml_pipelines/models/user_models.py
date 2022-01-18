'''
    Define all user models
    (models that take a user_id as input
    and return a vector embedding)
'''
import tensorflow as tf

class SimpleUserModel(tf.keras.Model):
    '''
        User model only based on an embedding layer
        that maps a user_id to an embedding vector
    '''
    def __init__(self, unique_users, user_embedding_size=32):
        super().__init__()
        self.user_id_lookup_layer = tf.keras.layers.StringLookup(
            vocabulary=unique_users,
            num_oov_indices=0,
            name='simple_user_model_id_lookup'
        )
        self.user_embedding_layer = tf.keras.layers.Embedding(
            self.user_id_lookup_layer.vocabulary_size(), 
            user_embedding_size,
            name='simple_user_model_embedding',
            trainable=True
        )
    def call(self, user_id):
        '''
            Takes of tensor of user_ids and returns their embeddings
        '''
        user_idx = self.user_id_lookup_layer(user_id)
        user_embedding = self.user_embedding_layer(user_idx)
        return user_embedding
