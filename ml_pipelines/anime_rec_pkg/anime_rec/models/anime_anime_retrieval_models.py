'''
    Anime Anime Retrieval
'''
import tensorflow as tf
import tensorflow_recommenders as tfrs

class AnimeAnimeRetrievalModel(tfrs.Model):
    '''
        Anime Anime Retrieval model
        Takes an anime model and list of all possible animes
        For each anime_id retrieves the best animes from 
        the set of all possible animes
    '''
    def __init__(self, anime_model, unique_anime_ids):
        super().__init__()

        self.anime_model = anime_model

        animes_ds = tf.data.Dataset.from_tensor_slices(unique_anime_ids)
        retrieval_metrics = tfrs.metrics.FactorizedTopK(
            candidates=animes_ds.batch(128).map(self.anime_model)
        )
        self.task = tfrs.tasks.Retrieval(
            metrics=retrieval_metrics
        )

    def compute_loss(self, data, training=False):
        '''
            Run retrieval task
        '''
        anchor_anime_embedding = self.anime_model(data["anime_id"])
        positive_anime_embeddings = self.anime_model(data["retrieved_anime_id"])

        return self.task(anchor_anime_embedding, positive_anime_embeddings, compute_metrics=not training)