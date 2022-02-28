'''
    User Anime List Ranking Models
    Define Models that take for each user
    a list of animes and a list of scores
    and predict scores such that the animes
    are ranked the same as in the ground truth
'''
import tensorflow as tf
import tensorflow_ranking as tfr

class UserAnimeRankingModel(tf.keras.Model):
        def __init__(
            self,
            user_model,
            anime_model,
            scoring_layer_size=32
        ):
            super().__init__()
            # Compute embeddings for animes.
            self.anime_model = anime_model

            # Compute embeddings for users.
            self.user_model = user_model

            # Compute predictions.
            self.scoring_layer = tf.keras.Sequential([
                tf.keras.layers.Dense(scoring_layer_size, activation="relu"),
                tf.keras.layers.Dense(1)
            ])
        
        def call(self, inputs):

            user_id = inputs['user_id']
            anime_id = inputs['anime_id']

            user_embedding = self.user_model(user_id)
            anime_embedding = self.anime_model(anime_id)

            pred_ratings = self.scoring_layer(tf.concat([user_embedding, anime_embedding], axis=-1))
            return pred_ratings
        

class UserAnimeListRankingModel(tf.keras.Model):
    '''
        Keras model that takes a user model and an anime model
        Computes the embeddings of a user and an anime
        Concatenate the two embeddings
        Pass the result through a neural network that computes final user anime pair score
    '''
    def __init__(
            self,
            user_model,
            anime_model,
            scoring_layer_size=32,
            topn=5,
            positive_threshold=8.0
    ):
        super().__init__()

        self.positive_threshold = positive_threshold

        self.classification_metrics = [
            tfr.keras.metrics.PrecisionMetric(topn=topn, name=f'Precision@{topn}'),
            tfr.keras.metrics.RecallMetric(topn=topn, name=f'Recall@{topn}'),
            tfr.keras.metrics.MeanAveragePrecisionMetric(topn=topn, name=f'MAP@{topn}'),
            tfr.keras.metrics.MRRMetric(name=f'MRR')
        ]

        self.non_classification_metrics = [
            tfr.keras.metrics.NDCGMetric(name=f'NDCG'),
            tf.keras.metrics.RootMeanSquaredError()
        ]

        # Compute embeddings for animes.
        self.anime_model = anime_model

        # Compute embeddings for users.
        self.user_model = user_model

        # Compute predictions.
        self.scoring_layer = tf.keras.Sequential([
            tf.keras.layers.Dense(scoring_layer_size, activation="relu"),
            tf.keras.layers.Dense(1)
        ])

    def train_step(self, data):
        
        x, y = data

        with tf.GradientTape() as tape:

            y_true = y
            y_true_binary = tf.cast(y_true >= self.positive_threshold, tf.int32)

            y_pred = self(x, training=True)

            loss = self.compiled_loss(y_true, y_pred, regularization_losses=self.losses)

        gradients = tape.gradient(loss, self.trainable_variables)
        self.optimizer.apply_gradients(zip(gradients, self.trainable_variables))

        self.compiled_metrics.update_state(y_true, y_pred)

        for classification_metric in self.classification_metrics:
            classification_metric.update_state(y_true_binary, y_pred)

        for non_classification_metric in self.non_classification_metrics:
            non_classification_metric.update_state(y_true, y_pred)

        return {m.name: m.result() for m in self.metrics}

    def test_step(self, data):
        
        x, y = data

        y_true = y
        y_true_binary = tf.cast(y_true >= self.positive_threshold, tf.int32)

        y_pred = self(x, training=False)
        y_pred = tf.cast(y_pred, dtype=tf.float32)

        self.compiled_loss(y_true, y_pred, regularization_losses=self.losses)
        self.compiled_metrics.update_state(y_true, y_pred)

        for classification_metric in self.classification_metrics:
            classification_metric.update_state(y_true_binary, y_pred)

        for non_classification_metric in self.non_classification_metrics:
            non_classification_metric.update_state(y_true, y_pred)

        return {m.name: m.result() for m in self.metrics}

    @property
    def metrics(self):
        return self.compiled_loss.metrics + \
               self.compiled_metrics.metrics + \
               self.classification_metrics + \
               self.non_classification_metrics
    
    def call(self, inputs):

        user_id = inputs['user_id']
        anime_id = inputs['anime_id']

        user_embedding = self.user_model(user_id)
        anime_embedding = self.anime_model(anime_id)

        pred_ratings = self.scoring_layer(tf.concat([user_embedding, anime_embedding], axis=-1))
        pred_ratings = tf.squeeze(pred_ratings, axis=-1)
        return pred_ratings


