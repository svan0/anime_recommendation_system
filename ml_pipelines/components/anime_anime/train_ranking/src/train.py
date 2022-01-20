'''
    Train anime anime ranking models
'''
import tensorflow as tf

from models.anime_models import SimpleAnimeModel
from models.anime_anime_ranking_models import AnimeAnimeScoringModel
from models.anime_anime_ranking_models import AnimeAnimePairClassificationModel

def get_anime_anime_pair_ranking_classification_model(
        list_anime,
        anime_embedding_size,
        scoring_layer_size,
        learning_rate,
        optimizer
):
    '''
        Creates the anime pair ranking classification model
    '''
    anime_model = SimpleAnimeModel(list_anime, anime_embedding_size)

    anime_pair_scoring_model = AnimeAnimeScoringModel(anime_model, scoring_layer_size)
    anime_anime_pair_classification_model = AnimeAnimePairClassificationModel(anime_pair_scoring_model)
    train_optimizer = tf.keras.optimizers.get(optimizer)
    train_optimizer.learning_rate = learning_rate

    anime_anime_pair_classification_model.compile(
        optimizer=train_optimizer,
        loss='binary_crossentropy',
        metrics=[
            tf.keras.metrics.BinaryAccuracy(),
            tf.keras.metrics.Precision(),
            tf.keras.metrics.Recall(),
            tf.keras.metrics.AUC()
        ]
    )
    return anime_anime_pair_classification_model

def train_anime_anime_pair_ranking_classification_model(
        list_anime,
        train_data,
        validation_data,
        hyperparameters
):
    '''
        Trains the anime pair ranking classification model
    '''
    model = get_anime_anime_pair_ranking_classification_model(
        list_anime,
        hyperparameters.get('anime_embedding_size'),
        hyperparameters.get('scoring_layer_size'),
        hyperparameters.get('learning_rate'),
        hyperparameters.get('optimizer')
    )

    max_num_epochs = hyperparameters.get('max_num_epochs')
    early_stop_num_epochs = hyperparameters.get('early_stop_num_epochs')

    early_stop_callback = tf.keras.callbacks.EarlyStopping(
        monitor='val_binary_accuracy',
        patience=early_stop_num_epochs,
        mode='max'
    )

    callbacks = [early_stop_callback]

    model.fit(
        train_data,
        epochs=max_num_epochs,
        validation_data=validation_data,
        callbacks=callbacks,
        verbose=2
    )

    return model
