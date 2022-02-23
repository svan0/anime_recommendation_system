'''
    Define retrieval models for user anime pairs
'''
import tensorflow as tf
import tensorflow_recommenders as tfrs

class UserAnimeRetrievalModel(tfrs.Model):
    '''
        User Anime Retrieval model
    '''
    def __init__(self, user_model, anime_model, unique_anime_ids):
        super().__init__()
        self.user_model = user_model
        self.anime_model = anime_model

        animes_ds = tf.data.Dataset.from_tensor_slices(unique_anime_ids)
        retrieval_metrics = tfrs.metrics.FactorizedTopK(
            candidates=animes_ds.batch(128).map(self.anime_model)
        )
        self.task = tfrs.tasks.Retrieval(
            metrics=retrieval_metrics
        )

    def compute_loss(self, features, training=False):
        '''
            Run retrieval task
        '''
        user_embeddings = self.user_model(features["user_id"])
        positive_anime_embeddings = self.anime_model(features["anime_id"])

        return self.task(user_embeddings, positive_anime_embeddings, compute_metrics=not training)
