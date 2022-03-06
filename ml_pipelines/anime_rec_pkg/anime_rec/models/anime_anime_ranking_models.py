'''
    Define Anime Anime Ranking models
    (models that take two anime_ids as input and predict
    a ranking score for the pair of animes
    )
'''
import tensorflow as tf

class AnimeAnimeScoringModel(tf.keras.Model):
    '''
        Anime Scoring model
        Computes anime embedding for each of the two input animes
        Concatenate the two embeddings, pass the result through a 
        neural network that outputs a final score
    '''
    def __init__(self, anime_model, scoring_layer_size=64):
        super().__init__()

        self.anime_model = anime_model
        self.scoring_layer = tf.keras.Sequential([
            tf.keras.layers.Dense(scoring_layer_size, activation='relu'),
            tf.keras.layers.Dense(1)    
        ])

    def call(self, data):
        '''
            Compute the anime pair score
            anime_id : initial anime
            retrieved_anime_id : anime to be scored. The higher the score
                the more relevant retrieved_anime_id is to the anchor anime
        '''
        anchor_anime_embedding = self.anime_model(data["anime_id"])
        relevant_anime_embedding = self.anime_model(data["retrieved_anime_id"])

        pred_score = self.scoring_layer(tf.concat([anchor_anime_embedding, relevant_anime_embedding], axis=-1))

        return pred_score

class AnimeAnimePairClassificationModel(tf.keras.Model):
    '''
        Classification model that trains the scoring model
        This model takes as input three anime_ids and a label
            anime_id : the initial anime
            retrieved_anime_id_1 : first anime to be scored
            retrieved_anime_id_2 : second anime to be score
            label : 1 if retrieved_anime_id_1 is more relevant to anchor_anime than retrieved_anime_id_2
                    0 else
        Model computes the two scores and return
        sigmoid(score1 - score2) as binary classification prediction
        
    '''
    def __init__(self, anime_scoring_model):
        super().__init__()

        self.anime_scoring_model = anime_scoring_model

    def call(self, data):

        pred_score_1 = self.anime_scoring_model({
            'anime_id' : data["anime_id"],
            'retrieved_anime_id' : data["retrieved_anime_id_1"]
        })

        pred_score_2 = self.anime_scoring_model({
            'anime_id' : data["anime_id"],
            'retrieved_anime_id' : data["retrieved_anime_id_2"]
        })

        classification_score = tf.math.sigmoid(pred_score_1 - pred_score_2)
        return classification_score
